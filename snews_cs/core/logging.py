"""
Modified version of Geoffrey Lentner's python 201 logger.

Ref: https://python-tutorial.dev/201/tutorial/logging.html
"""
import os
import time
from datetime import date
from socket import gethostname
from hop import Stream
from logging import (
    getLogger,
    NullHandler,
    Handler,
    Formatter,
    FileHandler,
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    CRITICAL,
)
from . import cs_utils

cs_utils.set_env(env_path)
broker = os.getenv("HOP_BROKER")
topic = os.getenv("SNEWPLOG_TOPIC")

HOST = gethostname()
log_date = date.today().strftime("%Y-%m-%d")
log_dir = os.getenv('SNEWSLOG')

if not log_dir:
    log_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../logs"))

# These should be exclusive, one or the other: fh or klh.
log_file = f"{log_dir}/snews_cs.log"
fh = FileHandler(log_file)
#
klh = SNEWPlog(broker, topic, auth)
#


formatter = Formatter(
    f"%(asctime)s on {HOST}\n" f"  %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

formatter.converter = time.gmtime

fh.setFormatter(formatter)

logger = getLogger("snews_cs")
logger.addHandler(NullHandler())

levels = {
    "debug": DEBUG,
    "info": INFO,
    "warning": WARNING,
    "error": ERROR,
    "critical": CRITICAL,
}


def initialize_logging(level):
    """Initialize top-level logger with the file handler and a `level`."""
    if fh not in logger.handlers:
        logger.addHandler(fh)
        logger.setLevel(levels.get(level))
        logger.propagate = False


class SNEWPlog(logging.Handler):
    """ Implement logging over kafka topic
    """

    def __init__(self, host: str, topic: str, auth: str=None):
        logging.Handler.__init__(self)
        self.auth = auth
        uri = f"{host}/{topic}"
        self.producer = Stream(until_eos=True, auth=self.auth).open(uri, "w")

    def emit(self, record):
        if record.name == 'kafka':
            return

        msg = self.format(record)
        self.producer.write(msg)

    def close(self):
        self.producer.close()
        logging.Handler.close(self)