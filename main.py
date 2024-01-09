import sys
import logging
import json
from rich import print
#from para_stats import init_script

def main():
    with open('data/raw/blackbox_cache.json') as f:
        raw = json.load(f)
    cleaned_list = []
    for idx, rnd in enumerate(raw): # each round query rnd: list in raw: list
        for metric in rnd: # each metric: dict in rnd: list
            try:
                metric["data"] = json.loads(metric["raw_data"])["data"]
            except TypeError:
                print("Caught type error, at index", idx)
                print(metric)
                SystemExit(0)
            metric.pop("raw_data")
        cleaned_list.append(rnd)


    with open('data/interim/blackbox_cleaned_test.json', 'w') as f:
        json.dump(cleaned_list, f, indent=4)

    print("Done!")

if __name__ == "__main__":
    # TODO: debug cmd line arguments, get rid of this later 
    # with open('all_round_ids.txt') as f:
    #     valid_rounds = [int(x) for x in f.read().strip().splitlines()]
    # start_id = valid_rounds[valid_rounds.index(37678)]
    # end_id = valid_rounds[valid_rounds.index(37678) + 2000]
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    # init_script(start_id, end_id)
    main()