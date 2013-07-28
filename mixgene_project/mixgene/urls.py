from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()

from webapp import views

urlpatterns = patterns('',
    url(r'^$', 'webapp.views.index', name='index'),
    url(r'^about$', 'webapp.views.about', name='about'),
    url(r'^contact$', 'webapp.views.contact', name='contact'),


    url(r'^experiments$', 'webapp.views.experiments', name='experiments'),
    url(r'^experiment/(?P<exp_id>\d+)/$', 'webapp.views.exp_details', name='exp_details'),

    url(r'^add_experiment$', 'webapp.views.add_experiment', name='add_experiment'),

    url(r'^create_experiment/', 'webapp.views.create_experiment', name='create_experiment'),
    url(r'^create_exp_instance/', 'webapp.views.create_exp_instance', name='create_exp_instance'),

    url(r'^upload_data/', 'webapp.views.upload_data', name='upload_data'),

    url(r'^auth/login/$', 'django.contrib.auth.views.login', {'template_name': 'auth/login.html'}),
    url(r'^auth/logout/$', 'django.contrib.auth.views.logout', {}),
    url(r'^auth/create_user/$', 'webapp.views.create_user', {}),

    # Examples:
    # url(r'^$', 'mixgene.views.home', name='home'),
    # url(r'^mixgene/', include('mixgene.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
)
