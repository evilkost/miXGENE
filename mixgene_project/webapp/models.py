from __builtin__ import staticmethod
from collections import defaultdict
import gzip
import os
import shutil
import cPickle as pickle
import hashlib
import logging

import pandas as pd

from django.db import models
from django.contrib.auth.models import User
from redis.client import StrictPipeline


from mixgene.settings import MEDIA_ROOT
from mixgene.util import get_redis_instance
from mixgene.redis_helper import ExpKeys

from environment.structures import GmtStorage, GeneSets
from webapp.tasks import auto_exec_task

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class CachedFile(models.Model):
    uri = models.TextField(default="")
    uri_sha = models.CharField(max_length=127, default="")
    dt_updated = models.DateTimeField(auto_now=True)

    def __unicode__(self):
        return u"cached file of %s, updated at %s" % (self.uri, self.dt_updated)

    def get_file_path(self):
        return '/'.join(map(str, [MEDIA_ROOT, 'data', 'cache', self.uri_sha]))

    def save(self, *args, **kwargs):
        self.uri_sha = hashlib.sha256(self.uri).hexdigest()
        super(CachedFile, self).save(*args, **kwargs)

    @staticmethod
    def update_cache(uri, path_to_real_file):
        res = CachedFile.objects.filter(uri=uri)
        if len(res) == 0:
            cf = CachedFile()
            cf.uri = uri
            cf.save()
        else:
            cf = res[0]

        shutil.copy(path_to_real_file, cf.get_file_path())

    @staticmethod
    def look_up(uri):
        res = CachedFile.objects.filter(uri=uri)
        if len(res) == 0:
            return None
        else:
            return res[0]


class Article(models.Model):
    author_user = models.ForeignKey(User)
    author_title = models.CharField(max_length=255)

    title = models.CharField(max_length=255)
    preview = models.TextField()
    content = models.TextField()

    dt_created = models.DateTimeField(auto_now_add=True)
    dt_updated = models.DateTimeField(auto_now=True)

    def to_dict(self, *args, **kwargs):
        return {
            "author_title": self.author_title,
            "title": self.title,
            "preview": self.preview,
            "content": self.content,
            "dt_created": str(self.dt_created),
            "dt_updated": str(self.dt_updated)
        }


class Experiment(models.Model):
    author = models.ForeignKey(User)

    # Obsolete
    status = models.TextField(default="created")

    dt_created = models.DateTimeField(auto_now_add=True)
    dt_updated = models.DateTimeField(auto_now=True)

    def __unicode__(self):
        return u"%s" % self.pk

    def __init__(self, *args, **kwargs):
        super(Experiment, self).__init__(*args, **kwargs)
        self._blocks_grouped_by_provided_type = None

    @staticmethod
    def get_exp_by_ctx(ctx):
        return Experiment.objects.get(pk=ctx["exp_id"])

    @staticmethod
    def get_exp_by_id(_pk):
        return Experiment.objects.get(pk=_pk)

    def execute(self):
        auto_exec_task.s(self, "root", is_init=True).apply_async()

    def post_init(self, redis_instance=None):
        ## TODO: RENAME TO init experiment and invoke on first save
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        pipe = r.pipeline()

        pipe.hset(ExpKeys.get_scope_creating_block_uuid_keys(self.pk), "root", None)
        pipe.sadd(ExpKeys.get_all_exp_keys_key(self.pk),[
            ExpKeys.get_exp_blocks_list_key(self.pk),
            ExpKeys.get_blocks_uuid_by_alias(self.pk),
            ExpKeys.get_scope_creating_block_uuid_keys(self.pk),
            ExpKeys.get_scope_key(self.pk, "root")
        ])
        pipe.execute()

    def get_ctx(self, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        key_context = ExpKeys.get_context_store_key(self.pk)
        pickled_ctx = r.get(key_context)
        if pickled_ctx is not None:
            ctx = pickle.loads(pickled_ctx)
        else:
            raise KeyError("Context wasn't found for exp_id: %s" % self.pk)
        return ctx

    def get_data_folder(self):
        return '/'.join(map(str, [MEDIA_ROOT, 'data', self.author.id, self.pk]))

    def get_data_file_path(self, filename, file_extension="csv"):
        if file_extension is not None:
            return self.get_data_folder() + "/" + filename + "." + file_extension
        else:
            return self.get_data_folder() + "/" + filename

    def get_all_scopes_with_block_uuids(self, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        return r.hgetall(ExpKeys.get_scope_creating_block_uuid_keys(self.pk))

    def store_block(self, block, new_block=False, redis_instance=None, dont_execute_pipe=False):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        if not isinstance(r, StrictPipeline):
            pipe = r.pipeline()
        else:
            pipe = r

        block_key = ExpKeys.get_block_key(block.uuid)
        if new_block:
            pipe.rpush(ExpKeys.get_exp_blocks_list_key(self.pk), block.uuid)
            pipe.sadd(ExpKeys.get_all_exp_keys_key(self.pk), block_key)
            pipe.hset(ExpKeys.get_blocks_uuid_by_alias(self.pk), block.base_name, block.uuid)

            # # TODO: refactor to scope.py
            # for var_name, data_type in block.provided_output.iteritems():
            #     self.register_variable(block.scope, block.uuid, var_name, data_type, pipe)

            if block.create_new_scope:
                pipe.hset(ExpKeys.get_scope_creating_block_uuid_keys(self.pk),
                          block.sub_scope_name, block.uuid)

            if block.scope_name != "root":
                # need to register in parent block
                parent_uuid = r.hget(ExpKeys.get_scope_creating_block_uuid_keys(self.pk), block.scope_name)
                parent = self.get_block(parent_uuid, r)

                # TODO: remove code dependency here
                parent.children_blocks.append(block.uuid)
                self.store_block(parent,
                                 new_block=False,
                                 redis_instance=pipe,
                                 dont_execute_pipe=True)

        pipe.set(block_key, pickle.dumps(block))

        if not dont_execute_pipe:
            pipe.execute()

        log.info("block %s was stored with state: %s [uuid: %s]",
              block.base_name, block.state, block.uuid)

    def change_block_alias(self, block, new_base_name):
        r = get_redis_instance()

        key = ExpKeys.get_blocks_uuid_by_alias(self.pk)
        pipe = r.pipeline()
        pipe.hdel(key, block.base_name)
        pipe.hset(key, new_base_name, block.uuid)
        pipe.execute()
        block.base_name = new_base_name
        self.store_block(block, redis_instance=r)

    @staticmethod
    def get_exp_from_request(request):
        exp_id = int(request.POST['exp_id'])
        return Experiment.objects.get(pk=exp_id)

    def get_block_by_alias(self, alias, redis_instance=None):
        """
            @type  alias: str
            @param alias: Human readable block name, can be altered

            @type  redis_instance: Redis
            @param redis_instance: Instance of redis client

            @rtype: GenericBlock
            @return: Block instance
        """
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        uuid = r.hget(ExpKeys.get_blocks_uuid_by_alias(self.pk), alias)
        return self.get_block(uuid, r)

    @staticmethod
    def get_block(block_uuid, redis_instance=None):
        """
            @type  block_uuid: str
            @param block_uuid: Block instance identifier

            @type  redis_instance: Redis
            @param redis_instance: Instance of redis client

            @rtype: GenericBlock
            @return: Block instance
        """
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        return pickle.loads(r.get(ExpKeys.get_block_key(block_uuid)))

    def get_scope_var_value(self, scope_var, redis_instance=None):
        """
            @type scope_var: webapp.scope.ScopeVar
        """
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        block = self.get_block(scope_var.block_uuid, r)
        return block.get_out_var(scope_var.var_name)

    @staticmethod
    def get_blocks(block_uuid_list, redis_instance=None):
        """
            @type  block_uuid_list: list
            @param block_uuid_list: List of Block instance identifier

            @type  redis_instance: Redis
            @param redis_instance: Instance of redis client

            @rtype: GenericBlock
            @return: Block instance
        """
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        return [(uuid, pickle.loads(r.get(ExpKeys.get_block_key(uuid))))
                for uuid in block_uuid_list]

    def get_all_block_uuids(self, redis_instance=None):
        """
        @type included_inner_blocks: list of str
        @param included_inner_blocks: uuids of inner blocks to be included

        @param redis_instance: Redis client

        @return: list of block uuids
        """
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        return r.lrange(ExpKeys.get_exp_blocks_list_key(self.pk), 0, -1) or []

    def get_block_aliases_map(self, redis_instance=None):
        """
        @param redis_instance: Redis

        @return: Map { uuid -> alias }
        @rtype: dict
        """
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        orig_map = r.hgetall(ExpKeys.get_blocks_uuid_by_alias(self.pk))
        return dict([
            (uuid, alias)
            for alias, uuid in orig_map.iteritems()
        ])

    def build_block_dependencies_by_scope(self, scope_name):
        """
            @return: { block: [ dependencies] }, root blocks have empty list as dependency
        """
        dependencies = {}
        # TODO: store in redis block uuids by scope
        for uuid, block in self.get_blocks(self.get_all_block_uuids()):
            if block.scope_name != scope_name:
                continue
            else:
                dependencies[str(block.uuid)] = map(str, block.get_input_blocks())

        return dependencies

    def get_meta_block_by_sub_scope(self, scope_name, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        block_uuid = r.hget(ExpKeys.get_scope_creating_block_uuid_keys(self.pk), scope_name)
        if not block_uuid:
            raise KeyError("Doesn't have a scope with name %s" % scope_name)
        else:
            return self.get_block(block_uuid, r)

    def get_dataflow_graphviz(self):
        from workflow.graphviz import root_from_exp
        import cStringIO as StringIO
        import pygraphviz as pgv

        dot_string = root_from_exp(self).to_dot()[0]
        # return dot_string

        # import ipdb; ipdb.set_trace()
        g = pgv.AGraph(dot_string)
        s = StringIO.StringIO()
        g.draw(path=s, format='svg', prog='dot')
        s.seek(0)
        result = s.read()
        return result


def delete_exp(exp):
    """
        We need to clean 3 areas:
            - keys in redis storage
            - uploaded and created files
            - delete exp object through ORM

        @param exp: Instance of Experiment  to be deleted
        @return: None

    """
    # redis
    r = get_redis_instance()
    all_exp_keys = ExpKeys.get_all_exp_keys_key(exp.pk)
    keys_to_delete = r.smembers(all_exp_keys)
    keys_to_delete.update(all_exp_keys)
    r.delete(keys_to_delete)

    # uploaded data
    data_files = UploadedData.objects.filter(exp=exp)
    for f in data_files:
        try:
            os.remove(f.data.path)
        except:
            pass
        f.delete()
    try:
        shutil.rmtree(exp.get_data_folder())
    except:
        pass

    # deleting an experiment
    exp.delete()

def content_file_name(instance, filename):
    return '/'.join(map(str, ['data', instance.exp.author.id, instance.exp.pk, filename]))


class UploadedData(models.Model):
    exp = models.ForeignKey(Experiment)
    var_name = models.CharField(max_length=255)
    filename = models.CharField(max_length=255, default="default")
    block_uuid = models.CharField(max_length=127, default="")

    data = models.FileField(null=True, upload_to=content_file_name)

    def __unicode__(self):
        return u":".join(map(str, [self.exp.pk, self.block_uuid, self.var_name]))


def gene_sets_file_name(instance, filename):
    return "broad_institute/%s" % filename


class UploadedFileWrapper(object):
    def __init__(self, uploaded_pk):
        self.uploaded_pk = uploaded_pk
        self.orig_name = ""

    @property
    def ud(self):
        return UploadedData.objects.get(pk=self.uploaded_pk)

    def get_file(self):
        return self.ud.data

    def get_as_data_frame(self):
        path = self.ud.data.path
        if self.orig_name[-2:] == "gz":
            with gzip.open(path) as inp:
                res = pd.DataFrame.from_csv(inp)
        else:
            res = pd.DataFrame.from_csv(path)

        return res

    def to_dict(self, *args, **kwargs):
        return {
            "filename": self.orig_name,
            "size": self.get_file().size,
        }


class BroadInstituteGeneSet(models.Model):
    section = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    UNIT_CHOICES = (
        ('entrez', 'Entrez IDs'),
        ('symbols', 'gene symbols'),
        ('orig', 'original identifiers'),
    )
    unit = models.CharField(max_length=31,
                            choices=UNIT_CHOICES,
                            default='entrez')
    gmt_file = models.FileField(null=False, upload_to=gene_sets_file_name)

    def __unicode__(self):
        return u"%s: %s. Units: %s" % (self.section, self.name, self.get_unit_display())

    def get_gene_sets(self):
        gene_sets = GeneSets(None, None)
        gene_sets.storage = GmtStorage(self.gmt_file.path)
        gene_sets.metadata["gene_units"] = self.unit
        return gene_sets

    @staticmethod
    def get_all_meta():
        res = []
        raw = BroadInstituteGeneSet.objects.order_by("section", "name", "unit").all()
        for record in raw:
            res.append({
                "pk": record.pk,
                "section": record.section,
                "name": record.name,
                "unit": record.unit,
                "str": str(record),
            })
        return res
