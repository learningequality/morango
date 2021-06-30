import os


class TestingRouter(object):
    """
    A router to control all database operations on models in the
    test application.
    """
    def db_for_read(self, *args, **kwargs):
        return os.environ.get("MORANGO_TEST_DATABASE", "default")

    def db_for_write(self, *args, **kwargs):
        return os.environ.get("MORANGO_TEST_DATABASE", "default")

    def allow_relation(self, *args, **kwargs):
        return True

    def allow_migrate(self, *args, **kwargs):
        return True
