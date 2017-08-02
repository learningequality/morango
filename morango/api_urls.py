from rest_framework import routers

from .api import CertificateViewSet

router = routers.SimpleRouter()
router.register(r'certificates', CertificateViewSet, base_name="certificates")
urlpatterns = router.urls