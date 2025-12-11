"""
Modified version of Geoffrey Lentner's python 201 logger.

Ref: https://python-tutorial.dev/201/tutorial/logging.html
"""

import os
import time
from datetime import date
from socket import gethostname
from hop import Stream
from hop.models import Blob

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

from .. import cs_utils

cs_utils.set_env()
logtopic = os.getenv("SNEWPLOG_TOPIC")

HOST = gethostname()
log_date = date.today().strftime("%Y-%m-%d")
log_dir = os.getenv("SNEWSLOG_DIR")

if not log_dir:
    log_dir = os.path.abspath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../logs")
    )

log_file = f"{log_dir}/snews_cs.log"

# Check if the directory exists, if not, create it
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Check if the log file exists, if not, create it
if not os.path.isfile(log_file):
    open(log_file, "w").close()

# These should be exclusive, one or the other: fh or klh.
log_file = f"{log_dir}/snews_cs.log"
fh = FileHandler(log_file)


class SNEWPlog(Handler):
    """Implement logging over kafka topic"""

    def __init__(self, topic: str, auth: str = None):
        Handler.__init__(self)
        self.auth = auth
        self.uri = topic
        if "kafka://" not in topic:
            self.uri = f"kafka://{topic}"

        try:
            self.producer = Stream(until_eos=True, auth=self.auth).open(
                self.uri, "w"
            )
        except ValueError as e:
            print(f"Problem establishing Kafka log connection: {e}")
        except Exception as e:
            print(f"Problem establishing Kafka log connection: {e}")

    def emit(self, record):
        if "kafka." in record.name:
            return

        msg = self.format(record)
        try:
            self.producer.write(Blob(msg))
        except Exception as e:
            print(f"Problem writing to Kafka log stream: {e}")

    def close(self):
        if self.producer:
            self.producer.close()
        Handler.close(self)


print(f"Initializing SNEWPlog with uri {logtopic}")
klh = SNEWPlog(logtopic, auth=True)
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
        logger.addHandler(fh)  # file
        logger.setLevel(levels.get(level))
        logger.propagate = False

    if klh not in logger.handlers:
        logger.addHandler(klh)  # kafka
        logger.setLevel(levels.get(level))
        logger.propagate = False
