import sys
import logging
from para_stats import init_script

if __name__ == "__main__":
    # TODO: debug cmd line arguments, get rid of this later 
    with open('all_round_ids.txt') as f:
        valid_rounds = [int(x) for x in f.read().strip().splitlines()]
    start_id = valid_rounds[valid_rounds.index(37678)]
    end_id = valid_rounds[valid_rounds.index(37678) + 2000]
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    init_script(start_id, end_id)