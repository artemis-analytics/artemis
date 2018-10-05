import logging
import sys
from artemis.logger import Logger

# logging.basicConfig(stream=sys.stdout,
#                    format=Logger.FMT,
#                    level=Logger.DEFAULT_LEVEL)
# logging.debug("Logging configured in package init")

sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(logging.Formatter(Logger.FMT))
sh.setLevel(logging.DEBUG)
logging.getLogger().addHandler(sh)
