from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from .models import round_table, metadata_table, db_metadata

class DatabaseLoader:
    def __init__(self, config) -> None:
        self.db_uri = config.db_uri
        self.db_ods_schema = config.db_ods_schema
        self.ods_table = config.db_ods_table

        self.engine = create_engine(self.db_uri, echo=False)
        self.db_metadata = db_metadata

    def __upsert_to_database(self, data_list: list, target_table) -> str:
        """
        Upserts values to specified table.

        TODO: needs error handling
        """

        self.db_metadata.create_all(bind=self.engine, tables=[target_table])

        with Session(self.engine) as session:
            insert_stmt = insert(target_table).values(data_list)

            # ideally, we get primary key dynamically, but that's probably not going to change here
            update_cols = {
                col.name: col
                for col in insert_stmt.excluded
                if col.name not in "round_id"
            }

            update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["round_id"], set_=update_cols
            )

            session.execute(update_stmt)
            session.commit()

        # TODO: figure out how to get the reflected Result rowcount lmfao
        result_statement = f"Inserted {len(data_list)} rows into {target_table}"

        return result_statement
    
    def db_upload_rounds(self, round_list: list):
        result = self.__upsert_to_database(round_list, round_table)
        return result

    def db_upload_metadata(self, metadata_list: list):
        result = self.__upsert_to_database(metadata_list, metadata_table)
        return result