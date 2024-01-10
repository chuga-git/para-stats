import json
import logging
from config import Config
from .api_fetch import APIFetch
from .transform import TransformData
from .db import DatabaseLoader


# TODO: figure out what to do with temp caching
WRITE_TO_CACHE = False


def read_raw_caches() -> tuple:
    with open("data/raw/metadata_cache.json") as f:
        metadata_raw = json.load(f)
    with open("data/raw/playercount_cache.json") as f:
        playercount_raw = json.load(f)
    with open("data/raw/blackbox_cache.json") as f:
        blackbox_raw = json.load(f)

    return (metadata_raw, playercount_raw, blackbox_raw)


class ETLInterface:
    def __init__(self) -> None:
        self._log = logging.getLogger(__name__)
        self._fetcher = APIFetch()
        self._transformer = TransformData()
        self._db = DatabaseLoader(Config)

    def update_metadata(self):
        new_round_id = self._fetcher.fetch_most_recent_round_id()
        old_round_id = (
            self._db.db_fetch_most_recent_round_id()
        )  # TODO: clean up these ugly ass method names :)

        if new_round_id > old_round_id:
            new_metadata_list = self._fetcher.fetch_roundlist_to_offset(old_round_id)
        else:
            new_metadata_list = []

        self._log.info(f"Found batch of {len(new_metadata_list)} rounds to update.")

        return new_metadata_list

    def fetch_missing_round_data(self) -> tuple:
        """Compares DB metadata table to the rounds table and queries missing round data"""
        # get the metadata that doesn't have a partner in the rounds table
        metadata_diff_list = self._db.db_fetch_metadata_difference()

        # BAD
        if metadata_diff_list is None:
            return None

        round_id_list = [entry["round_id"] for entry in metadata_diff_list]

        # get the missing data
        playercount_list, raw_blackbox_list = self._fetcher.fetch_round_data_bulk(
            round_id_list
        )

        # hmm yes give me several hundred megabytes of text please
        if WRITE_TO_CACHE:
            with open("data/raw/playercount_cache.json", "w") as f:
                json.dump(playercount_list, f)

            with open("data/raw/blackbox_cache.json", "w") as f:
                json.dump(raw_blackbox_list, f)

        # collect them into a list for upload
        collected_round_list = self.prep_rounds(
            metadata_diff_list, playercount_list, raw_blackbox_list
        )

        return collected_round_list

    def prep_rounds(
        self, metadata_list: list, playercount_list: list, raw_blackbox_list: list
    ) -> list:
        """Cleans and returns list of rounds ready for upload"""
        transform_data = TransformData()
        collected_round_list = transform_data.collect_round_batch(
            metadata_list, playercount_list, raw_blackbox_list
        )

        if WRITE_TO_CACHE:
            with open("data/processed/processed_rounds_cache.json", "w") as f:
                json.dump(collected_round_list, f)

        return collected_round_list

    def load_metadata(self, metadata_list: list) -> str:
        """Uploads metadata list to the database"""
        self._log.info("Uploading metadata...")

        result = self._db.db_upload_metadata(metadata_list)

        return result

    def load_rounds(self, collected_round_list: list) -> str:
        """Uploads collected round list to the database"""
        result = self.db.db_upload_rounds(collected_round_list)

        return result


def init_script():
    interface = ETLInterface()

    print("Checking for new rounds...")

    # check if we need to update the master metadata list
    metadata_to_update = interface.update_metadata()

    # send it to the database
    if metadata_to_update:
        print(interface.load_metadata(metadata_to_update))

    print("Updating round playercount and blackbox data...")

    # fill in round data with what we're missing
    round_data_to_upload = interface.fetch_missing_round_data()

    if round_data_to_upload is None:
        print("Nothing to update!")
        # :)
        SystemExit(0)

    print(f"Collected {len(round_data_to_upload)} rounds to update...")

    upload_result = interface.load_rounds(round_data_to_upload)

    print(upload_result)

    print("Done!")
