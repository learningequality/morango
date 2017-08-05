from django.conf.urls import include, url

urlpatterns = [

    url(r'^api/morango/v1/', include('morango.api.urls')),

]
