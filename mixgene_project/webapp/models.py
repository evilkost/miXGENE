from __builtin__ import staticmethod
import os
import shutil
import cPickle as pickle
import hashlib

from django.db import models
from django.contrib.auth.models import User

from mixgene.settings import MEDIA_ROOT
from mixgene.util import get_redis_instance
from mixgene.redis_helper import ExpKeys
from mixgene.util import dyn_import


# Create your models here.

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
                  [key_context, key_context_version,
                   ExpKeys.get_exp_blocks_list_key(self.e_id)])

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

    def store_block(self, block, new_block=False, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        pipe = r.pipeline()
        block_key = ExpKeys.get_block_key(block.uuid)
        if new_block:
            pipe.rpush(ExpKeys.get_exp_blocks_list_key(self.e_id), block.uuid)
            pipe.sadd(ExpKeys.get_all_exp_keys_key(self.e_id), block_key)

        pipe.set(block_key, pickle.dumps(block))
        pipe.execute()

        print "block %s was stored with state: %s" % (block.uuid, block.state)

    def get_block(self, block_uuid, redis_instance=None):
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

    def get_all_block_uuids(self, include_inner_blocks=False, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        return r.lrange(ExpKeys.get_exp_blocks_list_key(self.e_id), 0, -1) or []


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
