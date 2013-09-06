import os, shutil
import cPickle as pickle

from django.db import models
from django.contrib.auth.models import User

from mixgene.settings import MEDIA_ROOT
from mixgene.util import get_redis_instance
from mixgene.redis_helper import ExpKeys
from mixgene.util import dyn_import


# Create your models here.
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
            ctx = dict() #{"error": "context wasn't stored"}
        return ctx

    def update_ctx(self, new_ctx, redis_instance=None):
        if redis_instance is None:
            r = get_redis_instance()
        else:
            r = redis_instance

        ctx = self.get_ctx(redis_instance=r)
        ctx.update(new_ctx)

        key_context = ExpKeys.get_context_store_key(self.e_id)
        r.set(key_context, pickle.dumps(ctx))


def delete_exp(exp):
    """
        @param exp: Instance of Experiment  to be deleted
        @return None
            We need to clean 3 areas:
            - keys in redis storage
            - uploaded and created files
            - delete exp object through ORM
    """
    # redis
    r = get_redis_instance()
    keys_to_delete = r.smembers(ExpKeys.get_all_exp_keys_key(exp.e_id))
    r.delete(keys_to_delete)
    r.delete(ExpKeys.get_all_exp_keys_key(exp.e_id))

    # uploaded data
    data_files = UploadedData.objects.filter(exp=exp)
    for f in data_files:
        try:
            os.remove(f.data.path)
        except:
            pass
        f.delete()
    try:
        to_del = '/'.join(map(str, [MEDIA_ROOT, 'data', exp.author.id, exp.e_id]))
        shutil.rmtree(to_del)
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
