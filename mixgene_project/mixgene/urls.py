from django.conf.urls import patterns, include, url

from django.contrib import admin

from django.conf import settings
from django.conf.urls.static import static

admin.autodiscover()

urlpatterns = patterns('',
    url(r'^$', 'webapp.views.index', name='index'),
    url(r'^about$', 'webapp.views.about', name='about'),
    url(r'^contact$', 'webapp.views.contact', name='contact'),

    url(r'^articles/(?P<article_id>\d+)$', 'webapp.views.article', name='article'),
    url(r'^articles/type=(?P<article_type>[\w|\d]+)$', 'webapp.views.articles', name='articles'),

    url(r'^constructor/(?P<exp_id>\d+)', 'webapp.views.constructor', name='constructor'),
    url(r'^experiments/ro/(?P<exp_id>\d+)', 'webapp.views.exp_ro', name='exp_ro'),

    url(r'^experiments/(?P<exp_id>\d+)/sub/(?P<sub>[\w|\d|_]+)?$',
        'webapp.views.exp_sub_resource', name="experiment_sub_resource"),

    url(r'^experiments/(?P<exp_id>\d+)/blocks/?$', 'webapp.views.blocks_resource', name="blocks_resource"),
    url(r'^experiments/(?P<exp_id>\d+)/blocks/(?P<block_uuid>[\w|\d]+)$',
        'webapp.views.block_resource', name="block_resource"),
    url(r'^experiments/(?P<exp_id>\d+)/blocks/(?P<block_uuid>[\w|\d]+)/actions/(?P<action_code>\w+)$',
        'webapp.views.block_resource', name="block_resource_with_action"),
    url(r'^experiments/(?P<exp_id>\d+)/blocks/(?P<block_uuid>[\w|\d]+)/(?P<field>\w+)$',
        'webapp.views.block_field_resource', name="block_field"),
    url(r'^experiments/(?P<exp_id>\d+)/blocks/(?P<block_uuid>[\w|\d]+)/invoke/(?P<method>\w+)$',
        'webapp.views.block_method', name="block_method"),
    url(r'^experiments/(?P<exp_id>\d+)/blocks/(?P<block_uuid>[\w|\d]+)/(?P<field>\w+)/(?P<format>[\w|\d|_]+)$',
        'webapp.views.block_field_resource', name="block_field_formatted"),


    url(r'^block_sub_page/(?P<exp_id>\d+)/(?P<block_uuid>[\w|\d]+)/(?P<sub_page>\w+)/',
        'webapp.views.block_sub_page', name="block_sub_page"),

    url(r'^experiments$', 'webapp.views.experiments', name='experiments'),
    url(r'^experiment/(?P<exp_id>\d+)/(?P<action>\w+)/$', 'webapp.views.alter_exp', name='alter_exp'),

    url(r'^add_experiment$', 'webapp.views.add_experiment', name='add_experiment'),

    url(r'^upload_data/', 'webapp.views.upload_data', name='upload_data'),

    url(r'^auth/login/$', 'django.contrib.auth.views.login',
        {'template_name': 'auth/login.html'}, name='login'),
    url(r'^auth/logout/$', 'django.contrib.auth.views.logout', {}),

    url(r'^auth/password_change/$', 'django.contrib.auth.views.password_change', {}),
    url(r'^auth/password_change/done/$', 'django.contrib.auth.views.password_change_done', name='password_change_done'),

    url(r'^auth/password_reset/$', 'django.contrib.auth.views.password_reset',
        {'template_name': 'auth/reset_form_request.html'},
        name='password_reset'
    ),
    url(r'^auth/password_reset/done/$', 'django.contrib.auth.views.password_reset_done',
        {'template_name': 'auth/reset_request_sent.html'},
        name='password_reset_done'
    ),

    url(r'^auth/reset/(?P<uidb64>[0-9A-Za-z_\-]+)/(?P<token>[0-9A-Za-z]{1,13}-[0-9A-Za-z]{1,20})/$',
        'django.contrib.auth.views.password_reset_confirm',
        {'template_name': 'auth/password_reset_confirm.html'},
        name='password_reset_confirm'
    ),
    url(r'^auth/reset/done/$', 'django.contrib.auth.views.password_reset_complete',
        {'template_name': 'auth/password_reset_done.html'},
        name='password_reset_complete'
    ),

    url(r'^auth/create_user/$', 'webapp.views.create_user', {}),

    # Examples:
    # url(r'^$', 'mixgene.views.home', name='home'),

    # url(r'^mixgene/', include('mixgene.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
) + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
if settings.DEBUG :
    urlpatterns += patterns('',
        (r'^media/(?P<path>.*)$', 'django.views.static.serve', {'document_root': settings.MEDIA_ROOT, 'show_indexes': True}),
    )