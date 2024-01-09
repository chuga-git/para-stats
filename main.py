import sys
import logging
from para_stats import init_script

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    init_script()