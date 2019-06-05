from django.conf.urls import include
from django.conf.urls import url

urlpatterns = [url(r"^api/morango/v1/", include("morango.api.urls"))]
