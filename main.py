import sys
import logging
from para_stats import init_script

if __name__ == "__main__":
    # TODO: debug cmd line arguments, get rid of this later 
    if len(sys.argv) > 1:
        start_round_id = int(sys.argv[1])
        end_round_id = int(sys.argv[2])
    else:
        start_round_id = 0
        end_round_id = 38723 

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    init_script(start_round_id, end_round_id)