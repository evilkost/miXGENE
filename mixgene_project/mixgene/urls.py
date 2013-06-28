from django.conf.urls import patterns, include, url

from django.contrib import admin
admin.autodiscover()

from webapp import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
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
