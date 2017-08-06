from rest_framework import routers

from .viewsets import CertificateViewSet, NonceViewSet, SyncSessionViewSet

router = routers.SimpleRouter()
router.register(r'certificates', CertificateViewSet, base_name="certificates")
router.register(r'nonces', NonceViewSet, base_name="nonces")
router.register(r'syncsessions', SyncSessionViewSet, base_name="syncsessions")
urlpatterns = router.urls