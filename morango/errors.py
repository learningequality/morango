from morango.utils import exception_path


class MorangoError(Exception):
    @classmethod
    def path(cls):
        return exception_path(cls)


class ModelRegistryNotReady(MorangoError):
    pass


class InvalidMorangoModelConfiguration(MorangoError):
    pass


class UnsupportedFieldType(MorangoError):
    pass


class MorangoCertificateError(MorangoError):
    pass


class CertificateScopeNotSubset(MorangoCertificateError):
    pass


class CertificateSignatureInvalid(MorangoCertificateError):
    pass


class CertificateIDInvalid(MorangoCertificateError):
    pass


class CertificateProfileInvalid(MorangoCertificateError):
    pass


class CertificateRootScopeInvalid(MorangoCertificateError):
    pass


class MorangoNonceError(MorangoError):
    pass


class NonceDoesNotExist(MorangoNonceError):
    pass


class NonceExpired(MorangoNonceError):
    pass


class MorangoServerDoesNotAllowNewCertPush(MorangoError):
    pass


class MorangoResumeSyncError(MorangoError):
    pass


class MorangoContextUpdateError(MorangoError):
    pass


class MorangoLimitExceeded(MorangoError):
    pass


class InvalidMorangoSourceId(MorangoError):
    pass


class MorangoInvalidFSICPartition(MorangoError):
    pass


class MorangoSkipOperation(MorangoError):
    pass


class MorangoDatabaseError(MorangoError):
    pass


class MorangoDirtyParent(MorangoError):
    pass


class MorangoMissingParent(MorangoError):
    pass
