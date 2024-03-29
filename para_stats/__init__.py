import logging
from config import Config
from .api_fetch import APIFetch
from .transform import TransformData
from .db import DatabaseLoader

class ETLInterface:
    def __init__(self, url:str = 'https://api.paradisestation.org/stats') -> None:
        self._log = logging.getLogger(__name__)
        self._fetcher = APIFetch(url)
        self._transformer = TransformData()
        self._db = DatabaseLoader(Config) # note: this should automatically create the tables from the schemas if they don't exist

    def update_metadata(self):
        new_round_id = self._fetcher.fetch_most_recent_round_id()
        old_round_id = self._db.db_fetch_most_recent_round_id()

        if new_round_id > old_round_id:
            new_metadata_list = self._fetcher.fetch_roundlist_to_offset(old_round_id)
        else:
            new_metadata_list = []

        self._log.info(f"Found {len(new_metadata_list)} metadata entries to update.")

        return new_metadata_list

    def fetch_missing_round_data(self) -> tuple:
        """Compares DB metadata table to the rounds table and queries missing round data"""
        # get the metadata that doesn't have a partner in the rounds table
        metadata_diff_list = self._db.db_fetch_metadata_difference()

        if metadata_diff_list is None:
            return None

        if len(metadata_diff_list) >= 500:
            print(f"WARNING: {len(metadata_diff_list) * 2} rounds need to be queried. This could break!")
            debug_inpt = input("Continue? (Y/N) > ")

            if debug_inpt != 'Y':
                raise SystemExit("User requested termination")

        round_id_list = [entry["round_id"] for entry in metadata_diff_list]
        
        # get the missing data
        playercount_list, raw_blackbox_list = self._fetcher.fetch_round_data_bulk(round_id_list)

        # collect them into a list for upload
        collected_round_list = self.prep_rounds(
            metadata_diff_list, 
            playercount_list, 
            raw_blackbox_list,
        )

        return collected_round_list

    def prep_rounds(
        self, metadata_list: list, playercount_list: list, raw_blackbox_list: list
    ) -> list:
        """Cleans and returns list of rounds ready for upload"""
        transform_data = TransformData()
        
        collected_round_list = transform_data.collect_round_batch(
            metadata_list, 
            playercount_list, 
            raw_blackbox_list,
        )

        return collected_round_list

    def load_metadata(self, metadata_list: list) -> str:
        """Uploads metadata list to the database"""
        self._log.info("Uploading metadata...")

        result = self._db.db_upload_metadata(metadata_list)

        return result

    def load_rounds(self, collected_round_list: list) -> str:
        """Uploads collected round list to the database"""
        result = self._db.db_upload_rounds(collected_round_list)

        return result


def init_script():
    interface = ETLInterface()

    print("Checking for new rounds...")

    # check if we need to update the master metadata list
    metadata_to_update = interface.update_metadata()

    # send it to the database
    if metadata_to_update:
        print(interface.load_metadata(metadata_to_update))
    else:
        print("Database metadata table is already up to date!")

    print("Updating round playercount and blackbox data...")

    # fill in round data with what we're missing
    round_data_to_upload = interface.fetch_missing_round_data()

    if round_data_to_upload is None:
        print("Nothing to update!")
        raise SystemExit("Run completed")

    print(f"Collected {len(round_data_to_upload)} rounds to update...")

    upload_result = interface.load_rounds(round_data_to_upload)

    print(upload_result)

    print("Done!")
