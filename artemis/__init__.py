import logging
import sys

FMT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

DEFAULT_LEVEL = logging.INFO

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.debug("Logging configured in package init")
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(logging.Formatter(FMT))
sh.setLevel(DEFAULT_LEVEL)
logging.getLogger().addHandler(sh)

