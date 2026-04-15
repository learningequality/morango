from django.urls import include, path

urlpatterns = [path("api/morango/v1/", include("morango.api.urls"))]
