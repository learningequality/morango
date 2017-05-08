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
