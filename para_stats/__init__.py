from config import Config
from .api_fetch import APIFetch
from .transform import TransformData
from .db import DatabaseLoader
import pickle

DEBUG = True
# TODO: need to make a batch job and single update job
def init_script(round_id):
    rounds_list = fetch_rounds(round_id)
    #rounds_df = prep_rounds(rounds_list)
    upload = load_rounds(rounds_list)
    print(upload)

def debug_write(round_list, fpath):
    if not DEBUG:
        return None
    with open(fpath, 'wb') as f:
        pickle.dump(round_list, f)
    print("DEBUG::pickled rounds list to", fpath)

def debug_load(fpath):
    if not DEBUG:
        return None
    with open(fpath, 'rb') as f:
        result = pickle.load(f)
    print("DEBUG::deserialized from", fpath)
    return result

def fetch_rounds(round_id):
    fetcher = APIFetch()
    round_list = fetcher.concurrent_whole_round_batch(round_id)
    return round_list

def prep_rounds(rounds_list):
    transform_data = TransformData(rounds_list)
    rounds_df = transform_data.make_rounds_df()

    return rounds_df

def load_rounds(rounds_df):
    db = DatabaseLoader(Config)
    rounds_upload = db.upload_round_list(rounds_df)

    return rounds_upload