import pwd
import os

SQLITE_VARIABLE_FILE_CACHE = os.path.join(pwd.getpwuid(os.getuid()).pw_dir,
                                          'SQLITE_MAX_VARIABLE_NUMBER.cache')

