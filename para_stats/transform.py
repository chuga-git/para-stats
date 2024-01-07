from pandas import json_normalize
from typing import List

class TransformData:
    def __init__(self, round_list: List) -> None:
        self.round_list = round_list

    def make_rounds_df(self):
        # normalize it at the top level to keep pandas from shitting columns all over the place
        # this entire process was poorly thought out
        df = json_normalize(self.round_list, max_level=0)
        return df