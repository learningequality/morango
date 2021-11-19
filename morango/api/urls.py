from rest_framework import routers

from .viewsets import BufferViewSet
from .viewsets import CertificateChainViewSet
from .viewsets import CertificateViewSet
from .viewsets import MorangoInfoViewSet
from .viewsets import NonceViewSet
from .viewsets import PublicKeyViewSet
from .viewsets import SyncSessionViewSet
from .viewsets import TransferSessionViewSet

router = routers.SimpleRouter()
router.register(r"certificates", CertificateViewSet, basename="certificates")
router.register(
    r"certificatechain", CertificateChainViewSet, basename="certificatechain"
)
router.register(r"nonces", NonceViewSet, basename="nonces")
router.register(r"syncsessions", SyncSessionViewSet, basename="syncsessions")
router.register(
    r"transfersessions", TransferSessionViewSet, basename="transfersessions"
)
router.register(r"buffers", BufferViewSet, basename="buffers")
router.register(r"morangoinfo", MorangoInfoViewSet, basename="morangoinfo")
router.register(r"publickey", PublicKeyViewSet, basename="publickey")
urlpatterns = router.urls
