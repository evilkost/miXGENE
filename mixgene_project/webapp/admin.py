from django.contrib import admin
from webapp.models import *

admin.site.register(Experiment)
admin.site.register(UploadedData)
admin.site.register(CachedFile)

class ArticleAdmin(admin.ModelAdmin):
    list_display = ('dt_created', 'author_user', 'author_title', 'title', 'preview')

admin.site.register(Article, ArticleAdmin)

class BroadInstituteGeneSetAdmin(admin.ModelAdmin):
    list_display = ('section', 'name', 'unit')

admin.site.register(BroadInstituteGeneSet, BroadInstituteGeneSetAdmin)
