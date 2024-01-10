import logging
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from .models import round_table, metadata_table, db_metadata


class DatabaseLoader:
    def __init__(self, config) -> None:
        self._log = logging.getLogger(__name__)
        self.db_uri = config.db_uri
        # self.db_ods_schema = config.db_ods_schema
        # self.ods_table = config.db_ods_table

        self.engine = create_engine(self.db_uri, echo=False)
        self.db_metadata = db_metadata

    def __upsert_to_database(self, data_list: list, target_table) -> str:
        """
        Upserts values to specified table. This needs chunking for some UNGODLY REASON!!!!!!!!!!!!!!!!!!

        TODO: needs error handling and to actually return the number of rows inserted :)
        """
        self._log.info(
            f"Starting upsert against {target_table} with list of len {len(data_list)}"
        )
        # FUCK!!!!
        CHUNKSIZE = 1000
        chunks = (len(data_list) // CHUNKSIZE) + 1

        self.db_metadata.create_all(bind=self.engine, tables=[target_table])

        with Session(self.engine) as session:
            for i in range(chunks):
                start_i = i * CHUNKSIZE
                end_i = min((i + 1) * CHUNKSIZE, len(data_list))

                if start_i >= end_i:
                    break

                chunk_iter = data_list[start_i:end_i]
                insert_stmt = insert(target_table).values(chunk_iter)

                # ideally, we get the primary key dynamically
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

                self._log.info(
                    f"Chunk of len {len(chunk_iter)} successfully committed, {chunks - i} to go"
                )

        result_statement = f"Inserted {len(data_list)} rows into {target_table}"

        return result_statement

    def db_upload_rounds(self, round_list: list):
        result = self.__upsert_to_database(round_list, round_table)
        return result

    def db_upload_metadata(self, metadata_list: list):
        result = self.__upsert_to_database(metadata_list, metadata_table)
        return result

    def db_fetch_round_ids(self) -> list[int]:
        """Gets ALL round_ids from db metadata table, sorted high to low"""
        with Session(self.engine) as session:
            stmt = (
                select(metadata_table.c["round_id"]).order_by(
                    metadata_table.c["round_id"].desc()
                )
            )

            result = session.execute(stmt)
            round_id_list = result.scalars().all()
        
        return round_id_list

    def __db_find_rounds_to_query(self):
        pass

    def __db_fetch_most_recent_round(self) -> int:
        """Gets most recent round_id from metadata table"""
        with Session(self.engine) as session:
            stmt = (
                select(metadata_table.c["round_id"]).order_by(
                    metadata_table.c["round_id"].desc()
                )
            ).limit(1)

            result = session.execute(stmt)
            round_id = result.fetchone()[0]

        return round_id
