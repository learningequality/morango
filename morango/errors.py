class MorangoError(Exception):
    pass


class InvalidMorangoModelConfiguration(MorangoError):
    pass


class UnsupportedFieldType(MorangoError):
    pass


class InvalidSyncableManager(MorangoError):
    pass


class InvalidSyncableQueryset(MorangoError):
    pass


class InvalidMPTTManager(MorangoError):
    pass


class InvalidMPTTQuerySet(MorangoError):
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
