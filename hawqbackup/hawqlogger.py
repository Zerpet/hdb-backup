import logging.handlers
from os.path import expanduser

FORMAT = "%(asctime)-15s - %(module)s.%(funcName)s - %(levelname)s - %(message)s"
LOG_FILE_NAME = expanduser('~') + '/hawq_backup.log'

LOG = logging.getLogger("hdb_logger")
LOG.addHandler(logging.handlers.RotatingFileHandler(LOG_FILE_NAME, maxBytes=128*1024*1024, backupCount=5))
LOG.setLevel(logging.INFO)
