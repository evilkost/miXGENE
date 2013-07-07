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

    status = models.TextField(default="created")

    dt_created = models.DateTimeField(auto_now_add=True)
    dt_updated = models.DateTimeField(auto_now=True)

    wfl_setup = models.TextField(default="")  ## json encoded setup for WorkflowLayout

    def __unicode__(self):
        return u"%s" % self.e_id
