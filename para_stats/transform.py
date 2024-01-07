from typing import List
import json


class TransformData:
    def __init__(self, round_list: List = None) -> None:
        # this whole situation is fucked
        self.round_list = round_list

    def clean_blackbox_response(self, blackbox_response: List) -> List:
        for entry in blackbox_response:
            entry["data"] = json.loads(entry["raw_data"])["data"]
            entry.pop("raw_data")

            if entry["key_name"] == "RND Production List":
                entry["data"] = entry["data"]["/list"]

            if entry["key_name"] == "high_research_level":
                # temp check to see if there's ever going to be more than one element in the list
                assert len(entry["data"]) == 1
                entry["data"] = next(iter(entry["data"]))

        return blackbox_response

    def collect_round_batch(
        self, round_metadata_list: list, blackbox_raw_list: list, playercount_list: list
    ) -> list:
        # we're getting three lists here and we want to clean and collect into one list
        # blackbox gets sent to the laundromat and the return gets slapped onto the metadata response
        # mutating the metadata list sucks ass but doing a deepcopy would suck ass even more

        # clean it up
        blackbox_list = [self.clean_blackbox_response(r) for r in blackbox_raw_list]

        for idx, metadata in enumerate(round_metadata_list):
            metadata["playercounts"] = playercount_list[idx]
            metadata["stats"] = blackbox_list[idx]

        print(
            "FETCH::Roundlist collected successfuly with length",
            len(round_metadata_list),
            "\n",
        )

        return round_metadata_list
