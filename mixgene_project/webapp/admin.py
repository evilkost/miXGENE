from django.contrib import admin
from webapp.models import *

admin.site.register(Experiment)


class UploadedDataAdmin(admin.ModelAdmin):
    list_display = ("exp", "var_name", "block_uuid", "data")

admin.site.register(UploadedData, UploadedDataAdmin)
admin.site.register(CachedFile)


class ArticleAdmin(admin.ModelAdmin):
    list_display = ('dt_created', 'article_type', 'author_user', 'author_title', 'title', 'preview')

admin.site.register(Article, ArticleAdmin)


class BroadInstituteGeneSetAdmin(admin.ModelAdmin):
    list_display = ('section', 'name', 'unit')

admin.site.register(BroadInstituteGeneSet, BroadInstituteGeneSetAdmin)


class ArbitraryUploadAdmin(admin.ModelAdmin):
    list_display = ('dt_updated', 'url', 'data')

admin.site.register(ArbitraryUpload, ArbitraryUploadAdmin)