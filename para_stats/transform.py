import logging
import json


class TransformData:
    def __init__(self, round_list: list = None) -> None:
        # this whole situation is fucked
        self._log = logging.getLogger(__name__)
        self.round_list = round_list

    def clean_blackbox_response(self, blackbox_response: list) -> dict:
        """
        Takes a raw blackbox response and cleans it up for insertion into DB. Returns a DICTIONARY!

        Output format:
        ```
        {
            "key_name":
            {
                data_1: value_1
            },
            "key_name2":
            {
                etc
            }
        }
        ```

        Input example format:
        one round's list of metrics
        ```
        [
            { # <--- entry = blackbox_response[i]
                "key_name": "admin_secrets_fun_used",
                "key_type": "tally",
                "version": 1,
                "raw_data": # this is a string
                "{
                    "data":
                    {
                        "Roll The Dice": 1
                    }
                }"
            },
            { # <--- entry = second loop
                etc
            },
        ]
        ```
        """

        if blackbox_response is None:
            return None

        cleaned_response = {}

        for entry in blackbox_response:
            if not isinstance(entry, dict):
                print(type(entry))
                print(entry)
                raise TypeError

            try:
                raw_data = json.loads(entry["raw_data"])
            except (json.JSONDecodeError, TypeError) as e:
                self._log.exception("Exception while decoding raw_data", e, exc_info=1)
                raise Exception(entry) from e

            # now it's deserialized and we should hopefully be able to index it directly
            data = raw_data["data"]

            # get rid of list index artifacts
            if entry["key_type"] == "associative":
                if len(data.keys()) == 1:
                    # get the only key in the dictionary as safely as possible
                    data = data[next(iter(data))]
                elif len(data.keys()) > 1:
                    # turn the list indices into... an actual list
                    data = list(data.values())
                else:
                    self._log.critical(
                        f"Handled bad associative list with body {entry}"
                    )
                    data = None

            if entry["key_name"] == "RND Production list":
                # should always be "/list"... SHOULD!
                data = data[next(iter(data))]

            cleaned_response[entry["key_name"]] = data

        return cleaned_response

    def collect_round_batch(
        self,
        round_metadata_list: list,
        playercount_list: list,
        blackbox_raw_list: list,
    ) -> list:
        cleaned_blackbox_list = []

        # this is much slower than the list comprehension but it needs to be baby proofed
        for raw_response in blackbox_raw_list:
            cleaned_response = self.clean_blackbox_response(raw_response)
            cleaned_blackbox_list.append(cleaned_response)

        for idx, metadata in enumerate(round_metadata_list):
            metadata["playercounts"] = playercount_list[idx]
            metadata["stats"] = cleaned_blackbox_list[idx]

        self._log.info(
            f"Roundlist collected successfuly with length {len(round_metadata_list)}"
        )

        return round_metadata_list
