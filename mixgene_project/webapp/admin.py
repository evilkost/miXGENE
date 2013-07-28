from django.contrib import admin
from webapp.models import Experiment, WorkflowLayout, UploadedData

admin.site.register(Experiment)
admin.site.register(WorkflowLayout)
admin.site.register(UploadedData)
