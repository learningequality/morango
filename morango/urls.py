from django.urls import include
from django.urls import path

urlpatterns = [path("api/morango/v1/", include("morango.api.urls"))]
