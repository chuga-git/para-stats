import pickle
import json
from config import Config
from .api_fetch import APIFetch
from .transform import TransformData
from .db import DatabaseLoader

DEBUG = True


def init_script(start_round_id: int, end_round_id: int):
    responses = fetch_rounds(start_round_id, end_round_id)
    prepped_round_list = prep_rounds(responses)
    upload = load_rounds(prepped_round_list)
    print(upload)


def fetch_rounds(start_round_id: int, end_round_id: int) -> tuple:
    fetcher = APIFetch()
    response_tuple = fetcher.fetch_whole_round_batch(start_round_id, end_round_id)

    # dump it
    with open("data/raw/metadata_cache.json", 'w') as f:
        json.dump(response_tuple[0], f)
        
    with open("data/raw/playercount_cache.json", 'w') as f:
        json.dump(response_tuple[1], f)

    with open("data/raw/blackbox_cache.json", 'w') as f:
        json.dump(response_tuple[2], f)

    return response_tuple


def prep_rounds(endpoint_responses: tuple) -> list:
    transform_data = TransformData()
    collected_round_list = transform_data.collect_round_batch(*endpoint_responses) # hopefully this preserves order

    with open("data/processed/processed_rounds_cache.json", 'w') as f:
        json.dump(collected_round_list, f)

    return collected_round_list


def load_rounds(round_list: list) -> str:
    db = DatabaseLoader(Config)
    rounds_upload = db.upload_round_list(round_list)

    return rounds_upload


"""
---------------------------
        pickle debug
---------------------------
"""


def debug_write(round_list, fpath):
    if not DEBUG:
        return None
    with open(fpath, "wb") as f:
        pickle.dump(round_list, f)
    print("DEBUG::pickled rounds list to", fpath)


def debug_load(fpath):
    if not DEBUG:
        return None
    with open(fpath, "rb") as f:
        result = pickle.load(f)
    print("DEBUG::deserialized from", fpath)
    return result
