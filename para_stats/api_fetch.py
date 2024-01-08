import logging
from typing import List, Dict
from .adapter import SessionAdapter
from concurrent.futures import ThreadPoolExecutor


class APIFetch:
    def __init__(self) -> None:
        self._log = logging.getLogger(__name__)
        self._adapter = SessionAdapter()

    def __fetch_roundlist_paged(self, offset_start: int, offset_end: int) -> List:
        """Generator for paginated retrieval of roundlist endpoint. Will return overlapping data."""

        def fetch_single_page(offset: int) -> List:
            return self._adapter.get(f"/roundlist?offset={offset}")

        result = fetch_single_page(offset_start)
        yield result

        while result[-1]["round_id"] > offset_end:
            result = fetch_single_page(result[-1]["round_id"])
            yield result

    def fetch_roundlist_batch(self, offset_start: int, offset_end: int) -> List:
        rounds_list = []

        for result in self.__fetch_roundlist_paged(offset_start, offset_end):
            rounds_list += result

        return rounds_list

    def fetch_blackbox(self, round_id: int) -> List:
        """Fetch blackbox stats for a single round. `raw_data` is processed on hand."""
        result = self._adapter.get(f"/blackbox/{round_id}")

        return result

    def fetch_playercounts(self, round_id: int) -> Dict[str, int]:
        """Returns playercount timestamps for `round_id`"""
        result = self._adapter.get(f"/playercounts/{round_id}")

        return result

    def fetch_metadata(self, round_id: int) -> Dict:
        """Returns metadata stats for `round_id`. This returns the same information as the roundlist endpoint."""
        result = self._adapter.get(f"/metadata/{round_id}")

        return result

    def fetch_single_round(self, round_id: int) -> Dict:
        """Debug method, needs refactoring in the future."""
        round_metadata = self.fetch_metadata(round_id)
        round_blackbox = self.fetch_blackbox(round_id)
        round_playercounts = self.fetch_playercounts(round_id)

        round_metadata["playercounts"] = round_playercounts
        round_metadata["stats"] = round_blackbox

        return [round_metadata]

    def fetch_whole_round_batch(self, offset_start: int, offset_end: int) -> tuple:
        """Get all queryable information from the most recent to the target `offset_end` round."""

        self._log.info("Starting whole round batch fetch")

        # grab the rounds we'll need to query and then get the rest of their info after
        round_metadata_list = self.fetch_roundlist_batch(offset_start, offset_end)
        valid_round_ids = [r["round_id"] for r in round_metadata_list]
        self._log.info(f"Metadata retrieved successfully with length {len(round_metadata_list)}")

        blackbox_list = [self.fetch_blackbox(r) for r in valid_round_ids]
        self._log.info(f"Blackbox data retrieved successfully with length {len(blackbox_list)}")

        playercount_list = [self.fetch_playercounts(r) for r in valid_round_ids]
        self._log.info(f"Playercount data retrieved successfully with length {len(playercount_list)}")

        return (round_metadata_list, blackbox_list, playercount_list)

    def get_most_recent_round_id(self):
        return self._adapter.get("/roundlist?offset=0")[0]["round_id"]


# --------------- do not cross, bad juju ahead ---------------

    def concurrent_whole_round_batch(self, offset_end: int) -> Dict:
        """This was a bad idea from the start and its implementation makes it a micro-DDOS machine..."""
        self._log.info("Starting concurrent get...")

        # TODO: pretty sure a generator expression is going to shit itself if we try to multithread it - have to test
        round_metadata_list = self.fetch_roundlist_batch(offset_end)
        self._log.info("got metadata list of len", len(round_metadata_list))
        valid_round_ids = [r["round_id"] for r in round_metadata_list]

        self._log.info("Starting concurrent fetch of catchup rounds")
        # TODO: make max workers a config parameter
        with ThreadPoolExecutor(max_workers=2) as pool:
            # this should be safe to double task
            raw_blackbox_responses = list(
                pool.map(self.fetch_blackbox, valid_round_ids)
            )
            playercount_list = list(pool.map(self.fetch_playercounts, valid_round_ids))

        blackbox_list = [
            self.clean_blackbox_response(i) for i in raw_blackbox_responses
        ]

        self._log.info(
            "Roundlist collected successfuly with length",
            len(round_metadata_list),
        )

        for idx, metadata in enumerate(round_metadata_list):
            metadata["playercounts"] = playercount_list[idx]
            metadata["stats"] = blackbox_list[idx]

        return round_metadata_list
