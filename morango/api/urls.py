from rest_framework import routers

from .viewsets import (BufferViewSet, CertificateChainViewSet,
                       CertificateViewSet, MorangoInfoViewSet, NonceViewSet,
                       PublicKeyViewSet, SyncSessionViewSet,
                       TransferSessionViewSet)

router = routers.SimpleRouter()
router.register(r'certificates', CertificateViewSet, base_name="certificates")
router.register(r'certificatechain', CertificateChainViewSet, base_name="certificatechain")
router.register(r'nonces', NonceViewSet, base_name="nonces")
router.register(r'syncsessions', SyncSessionViewSet, base_name="syncsessions")
router.register(r'transfersessions', TransferSessionViewSet, base_name="transfersessions")
router.register(r'buffers', BufferViewSet, base_name="buffers")
router.register(r'morangoinfo', MorangoInfoViewSet, base_name="morangoinfo")
router.register(r'publickey', PublicKeyViewSet, base_name="publickey")
urlpatterns = router.urls
