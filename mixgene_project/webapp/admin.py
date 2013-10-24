from django.contrib import admin
from webapp.models import *

admin.site.register(Experiment)
admin.site.register(WorkflowLayout)
admin.site.register(UploadedData)
admin.site.register(CachedFile)
