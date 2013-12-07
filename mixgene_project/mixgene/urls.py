from django.conf.urls import patterns, include, url

from django.contrib import admin

from django.conf import settings
from django.conf.urls.static import static

admin.autodiscover()

from webapp import views

urlpatterns = patterns('',
    url(r'^$', 'webapp.views.index', name='index'),
    url(r'^about$', 'webapp.views.about', name='about'),
    url(r'^contact$', 'webapp.views.contact', name='contact'),

    url(r'^constructor/(?P<exp_id>\d+)', 'webapp.views.constructor', name='constructor'),
    url(r'^add_block', 'webapp.views.add_block', name='add_block'),
    url(r'^render_block', 'webapp.views.render_block', name='render_block'),
    url(r'^update_block', 'webapp.views.update_block', name='update_block'),

    url(r'^block_sub_page/(?P<exp_id>\d+)/(?P<block_uuid>[\w|\d]+)/(?P<sub_page>\w+)/',
        'webapp.views.block_sub_page', name="block_sub_page"),

    url(r'^experiments$', 'webapp.views.experiments', name='experiments'),
    url(r'^experiment/(?P<exp_id>\d+)/$', 'webapp.views.exp_details', name='exp_details'),
    url(r'^experiment/(?P<exp_id>\d+)/(?P<action>\w+)/$', 'webapp.views.alter_exp', name='alter_exp'),

    url(r'^add_experiment$', 'webapp.views.add_experiment', name='add_experiment'),

    #url(r'^create_experiment/(?P<layout_id>\d+)', 'webapp.views.create_experiment', name='create_experiment'),

    url(r'^upload_data/', 'webapp.views.upload_data', name='upload_data'),
    #url(r'^geo_fetch_data/', 'webapp.views.geo_fetch_data', name='geo_fetch_data'),

    url(r'^auth/login/$', 'django.contrib.auth.views.login', {'template_name': 'auth/login.html'}),
    url(r'^auth/logout/$', 'django.contrib.auth.views.logout', {}),
    url(r'^auth/create_user/$', 'webapp.views.create_user', {}),


    url(r'^get_flot_2d_scatter/(?P<exp_id>\d+)/(?P<filename>.*)$', 'webapp.views.get_flot_2d_scatter', name='get_flot_2d_scatter'),
    url(r'^get_csv_as_table/(?P<exp_id>\d+)/(?P<filename>.*)$', 'webapp.views.get_csv_as_table', name='get_csv_as_table'),

    url(r'^get_gse_samples_info/(?P<exp_id>\d+)/(?P<block_uuid>.*)$', 'webapp.views.get_gse_samples_info',
        name='get_gse_samples_info'),
    # Examples:
    # url(r'^$', 'mixgene.views.home', name='home'),
    # url(r'^mixgene/', include('mixgene.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
) + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
