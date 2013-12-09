from __builtin__ import staticmethod
from collections import defaultdict
import os
import shutil
import cPickle as pickle
import hashlib

from django.db import models
from django.contrib.auth.models import User
from redis.client import StrictPipeline

from mixgene.settings import MEDIA_ROOT
from mixgene.util import get_redis_instance
from mixgene.redis_helper import ExpKeys
from mixgene.util import dyn_import
from workflow.structures import GmtStorage


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


class WorkflowLayout(models.Model):
    w_id = models.AutoField(primary_key=True)
    wfl_class = models.TextField(null=True)  ## i.e.: 'workflow.layout.SampleWfL'

    title = models.TextField(default="")
    description = models.TextField(default="")

    def __unicode__(self):
        return u"%s" % self.title

    def get_class_instance(self):
        wfl_class = dyn_import(self.wfl_class)
        return wfl_class()


class Experiment(models.Model):
    e_id = models.AutoField(primary_key=True)
    workflow = models.ForeignKey(WorkflowLayout)
    author = models.ForeignKey(User)

    """
        TODO: use enumeration
        status evolution:
        1. created
        2. initiated [ not implemented yet, currently 1-> 3 ]
        3. configured
        3. running
        4. done OR
        5. failed
    """
    status = models.TextField(default="created")

    dt_created = models.DateTimeField(auto_now_add=True)
    dt_updated = models.DateTimeField(auto_now=True)

    # currently not used
    wfl_setup = models.TextField(default="")  ## json encoded setup for WorkflowLayout

    def __unicode__(self):
        return u"%s" % self.e_id

    def __init__(self, *args, **kwargs):
        super(Experiment, self).__init__(*args, **kwargs)
        self._blocks_grouped_by_provided_type = None

    @staticmethod
    def get_exp_by_ctx(ctx):
        return Experiment.objects.get(e_id=ctx["exp_id"])

    def init_ctx(self, ctx, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        key_context = ExpKeys.get_context_store_key(self.e_id)
        key_context_version = ExpKeys.get_context_version_key(self.e_id)

        pipe = r.pipeline()

        # FIXME: replace with MSET
        pipe.set(key_context, pickle.dumps(ctx))
        pipe.set(key_context_version, 0)

        pipe.sadd(ExpKeys.get_all_exp_keys_key(self.e_id),
                  [key_context,
                   key_context_version,
                   ExpKeys.get_exp_blocks_list_key(self.e_id),
                   ExpKeys.get_blocks_uuid_by_alias(self.e_id),
                   ExpKeys.get_scope_vars_keys(self.e_id),
                   ExpKeys.get_scope_creating_block_uuid_keys(self.e_id),
                  ])

        pipe.execute()

    def get_ctx(self, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        key_context = ExpKeys.get_context_store_key(self.e_id)
        pickled_ctx = r.get(key_context)
        if pickled_ctx is not None:
            ctx = pickle.loads(pickled_ctx)
        else:
            raise KeyError("Context wasn't found for exp_id: %s" % self.e_id)
        return ctx

    def update_ctx(self, new_ctx, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        key_context = ExpKeys.get_context_store_key(self.e_id)
        key_context_version = ExpKeys.get_context_version_key(self.e_id)

        result = None
        lua = """
        local ctx_version = tonumber(ARGV[1])
        local actual_version = redis.call('GET', KEYS[1])
        actual_version = tonumber(actual_version)
        local result = "none"
        if ctx_version == actual_version then
            redis.call('SET', KEYS[1], actual_version + 1)
            redis.call('SET', KEYS[2], ARGV[2])
            result = "ok"
        else
            result = "fail"
        end
        return result
        """
        safe_update = r.register_script(lua)
        # TODO: checkpoint for repeat if lua check version fails
        retried = 0
        while result != "ok" and retried < 4:
            retried = 1
            ctx_version, pickled_ctx = r.mget(key_context_version, key_context)
            if pickled_ctx is not None:
                ctx = pickle.loads(pickled_ctx)
            else:
                raise KeyError("Context wasn't found for exp_id: %s" % self.e_id)

            ctx.update(new_ctx)
            # TODO: move lua to dedicated module or singletone load in redis helper
            #  keys: ctx_version_key, ctx_key
            #  args: ctx_version,     ctx

            result = safe_update(
                keys=[key_context_version, key_context],
                args=[ctx_version, pickle.dumps(ctx)])

        if result != "ok":
            raise Exception("Failed to update context")

    def get_data_folder(self):
        return '/'.join(map(str, [MEDIA_ROOT, 'data', self.author.id, self.e_id]))

    def get_data_file_path(self, filename, file_extension="csv"):
        if file_extension is not None:
            return self.get_data_folder() + "/" + filename + "." + file_extension
        else:
            return self.get_data_folder() + "/" + filename

    def validate(self, request):
        new_ctx, errors = self.workflow.get_class_instance().validate_exp(self, request)
        if errors is None:
            self.status = "configured"
        else:
            self.status = "initiated"
        self.update_ctx(new_ctx)
        self.save()

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
            pipe.rpush(ExpKeys.get_exp_blocks_list_key(self.e_id), block.uuid)
            pipe.sadd(ExpKeys.get_all_exp_keys_key(self.e_id), block_key)

            for var_name, data_type in block.provided_objects.iteritems():
                self.register_variable(block.scope, block.uuid, var_name, data_type, pipe)

            if block.create_new_scope:
                #import ipdb; ipdb.set_trace()
                for var_name, data_type in block.provided_objects_inner.iteritems():
                    self.register_variable(block.sub_scope, block.uuid, var_name, data_type, pipe)

                pipe.hset(ExpKeys.get_scope_creating_block_uuid_keys(self.e_id),
                          block.sub_scope, block.uuid)

            if block.scope != "root":
                # need to register in parent block
                parent_uuid = r.hget(ExpKeys.get_scope_creating_block_uuid_keys(self.e_id),
                                     block.scope)
                parent = self.get_block(parent_uuid, r)
                #import ipdb; ipdb.set_trace()
                parent.children_blocks.append(block.uuid)
                self.store_block(parent,
                                 new_block=False,
                                 redis_instance=pipe,
                                 dont_execute_pipe=True)

        pipe.set(block_key, pickle.dumps(block))
        pipe.hset(ExpKeys.get_blocks_uuid_by_alias(self.e_id), block.base_name, block.uuid)

        if not dont_execute_pipe:
            pipe.execute()

        print "block %s was stored with state: %s" % (block.uuid, block.state)

    @staticmethod
    def get_exp_from_request(request):
        exp_id = int(request.POST['exp_id'])
        return Experiment.objects.get(e_id=exp_id)

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

        uuid = r.hget(ExpKeys.get_blocks_uuid_by_alias(self.e_id), alias)
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

    def group_blocks_by_provided_type(self, included_inner_blocks=None, redis_instance=None):
        if self._blocks_grouped_by_provided_type is not None:
            return self._blocks_grouped_by_provided_type
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        uuid_list = self.get_all_block_uuids(included_inner_blocks, r);

        self._blocks_grouped_by_provided_type = defaultdict(list)
        for uuid in uuid_list:
            block = self.get_block(uuid, r)
            provided = block.get_provided_objects()
            for data_type, field_name in provided.iteritems():
                self._blocks_grouped_by_provided_type[data_type].append(
                    (uuid, block.base_name, field_name)
                )
        return self._blocks_grouped_by_provided_type

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

        return r.lrange(ExpKeys.get_exp_blocks_list_key(self.e_id), 0, -1) or []

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

        orig_map = r.hgetall(ExpKeys.get_blocks_uuid_by_alias(self.e_id))
        return dict([
            (uuid, alias)
            for alias, uuid in orig_map.iteritems()
        ])

    def get_registered_variables(self, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        variables = []
        for key, val in r.hgetall(ExpKeys.get_scope_vars_keys(self.e_id)).iteritems():
            #scope, uuid, var_name, var_data_type = pickle.loads(val)
            variables.append(pickle.loads(val))

        return variables

    def get_visible_variables(self, scopes=None, data_types=None, redis_instance=None):
        if scopes is None:
            scopes = ["root"]

        scopes = set(scopes)
        scopes.add("root")

        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        all_variables = r.hgetall(ExpKeys.get_scope_vars_keys(self.e_id))
        visible = []
        for key, val in all_variables.iteritems():
            scope, uuid, var_name, var_data_type = pickle.loads(val)
            if scope not in scopes:
                continue

            if data_types is None or var_data_type in data_types:
                visible.append((uuid, var_name))

        return visible

    def register_variable(self, scope, block_uuid, var_name, data_type, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        record = pickle.dumps((scope, block_uuid, var_name, data_type))
        r.hset(ExpKeys.get_scope_vars_keys(self.e_id), "%s:%s" % (block_uuid, var_name), record)


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
    all_exp_keys = ExpKeys.get_all_exp_keys_key(exp.e_id)
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

    # workflow specific operations
    wfl_class = dyn_import(exp.workflow.wfl_class)
    wf = wfl_class()
    wf.on_delete(exp)

    # deleting an experiment
    exp.delete()



def content_file_name(instance, filename):
    return '/'.join(map(str, ['data', instance.exp.author.id, instance.exp.e_id, filename]))


class UploadedData(models.Model):
    exp = models.ForeignKey(Experiment)
    var_name = models.CharField(max_length=255)
    filename = models.CharField(max_length=255, default="default")
    data = models.FileField(null=True, upload_to=content_file_name)

    def __unicode__(self):
        return u"%s:%s" % (self.exp.e_id, self.var_name)


def gene_sets_file_name(instance, filename):
    return "broad_institute/%s" % filename


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
        gene_sets_s = GmtStorage(self.gmt_file.path)
        gene_sets = gene_sets_s.load()
        gene_sets.units = self.unit
        return gene_sets
