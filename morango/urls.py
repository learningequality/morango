from django.conf.urls import patterns, include, url

urlpatterns = patterns('',

    url(r'^api/morango/v1/', include('morango.api_urls')),

)
