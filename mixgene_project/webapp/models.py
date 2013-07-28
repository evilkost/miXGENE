from django.db import models

from django.contrib.auth.models import User

# Create your models here.
class WorkflowLayout(models.Model):
    w_id = models.AutoField(primary_key=True)
    wfl_class = models.TextField(null=True)  ## i.e.: 'workflow.layout.SampleWfL'

    def __unicode__(self):
        return u"%s" % self.w_id


class Experiment(models.Model):
    e_id = models.AutoField(primary_key=True)
    workflow = models.ForeignKey(WorkflowLayout)
    author = models.ForeignKey(User)

    """
        status evolution:
        1. created
        2. configured
        3. running
        4. done OR
        5. failed
    """
    status = models.TextField(default="created")


    dt_created = models.DateTimeField(auto_now_add=True)
    dt_updated = models.DateTimeField(auto_now=True)

    wfl_setup = models.TextField(default="")  ## json encoded setup for WorkflowLayout

    def __unicode__(self):
        return u"%s" % self.e_id


def content_file_name(instance, filename):
    return '/'.join(map(str, ['data', instance.exp.author.id, instance.exp.e_id, filename]))

class UploadedData(models.Model):
    exp = models.ForeignKey(Experiment)
    var_name = models.CharField(max_length=255)
    filename = models.CharField(max_length=255, default="default")
    data = models.FileField(null=True, upload_to=content_file_name)

    def __unicode__(self):
        return u"%s:%s" % (self.exp.e_id, self.var_name)
