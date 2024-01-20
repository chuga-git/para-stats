import logging
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from .models import round_table, metadata_table, db_metadata


class DatabaseLoader:
    def __init__(self, config, chunksize: int = 1000) -> None:
        """Handler for interacting with database.

        Args:
            config (Config): Pulls target table, schema, and DB URI from set env vars.
            chunksize (int, optional): Number of rows to push to database at a time. Larger batches will hit the Postgre upload limit and be generally error-prone and unperformant. Defaults to 1000.
        """
        self._log = logging.getLogger(__name__)
        self.db_uri = config.db_uri
        self.db_ods_schema = config.db_ods_schema

        self.CHUNKSIZE = chunksize
        self.engine = create_engine(self.db_uri, echo=False)
        self.db_metadata = db_metadata

        # FIXME: shouldn't have to specify the tables list here since they're already registed to the same metadata object that we're importing from the other file
        self.db_metadata.create_all(bind=self.engine, tables=[round_table, metadata_table])

    def __upsert_to_database(self, data_list: list, target_table) -> str:
        """
        Upserts values to specified table. Chunking algorithm partially derived from the Pandas to_sql() implementation.

        """
        self._log.info(
            f"Starting upsert against {target_table} with list of len {len(data_list)}"
        )

        num_chunks = (len(data_list) // self.CHUNKSIZE) + 1

        with Session(self.engine) as session:
            inserted_rowcount = 0
            for i in range(num_chunks):
                start_i = i * self.CHUNKSIZE
                end_i = min((i + 1) * self.CHUNKSIZE, len(data_list))

                if start_i >= end_i:
                    break

                chunk_iter = data_list[start_i:end_i]
                chunk_iter_len = len(chunk_iter)
                insert_stmt = insert(target_table).values(chunk_iter)

                # ideally, we get the primary key dynamically
                update_cols = {
                    col.name: col
                    for col in insert_stmt.excluded
                    if col.name not in "round_id"
                }

                update_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=["round_id"], 
                    set_=update_cols,
                )

                session.execute(update_stmt)

                inserted_rowcount += chunk_iter_len

                session.commit()
                
                self._log.info(
                    f"Chunk of len {chunk_iter_len} successfully committed, {num_chunks - i} to go"
                )

        result_statement = f"Inserted {inserted_rowcount} rows into {target_table}"

        return result_statement

    def db_upload_rounds(self, round_list: list) -> str:
        """Upserts compiled round data into db rounds table"""
        result = self.__upsert_to_database(round_list, round_table)
        return result

    def db_upload_metadata(self, metadata_list: list) -> str:
        """Upserts compiled metadata into db metadata table"""
        result = self.__upsert_to_database(metadata_list, metadata_table)
        return result

    def db_fetch_round_ids(self) -> list[int]:
        """Pulls all round_id entries from db metadata table, sorted high to low"""

        self._log.info("Starting fetch of all round_ids from metadata table")

        with Session(self.engine) as session:
            stmt = (
                select(metadata_table.c["round_id"]).order_by(
                    metadata_table.c["round_id"].desc()
                )
            )

            result = session.execute(stmt)
            
            
            round_id_list = result.scalars().all()

        return round_id_list

    def db_fetch_metadata_difference(self) -> list[dict]:
        """
        Returns metadata rows that do not exist in the collected round data table. 
        """

        self._log.info("Pulling metadata for difference comparison")

        with Session(self.engine) as session:
            exists_stmt = (
                select(metadata_table.c["round_id"])
                .where(round_table.c["round_id"] == metadata_table.c["round_id"])
            ).exists()

            stmt = (
                # binary negation for NOT EXISTS
                select(metadata_table).where(~exists_stmt)
            )

            result = session.execute(stmt)

            # dict constructor SHOULD(!) play nice with the snowflake named tuple since it already implements _asdict()
            if not result:
                self._log.warn("Difference query returned empty list.")
                return None
            
            metadata_list = [dict(row) for row in result.mappings()]

        self._log.info(f"Successfully queried metadata-difference list of len {len(metadata_list)}")

        return metadata_list

    def db_fetch_most_recent_round_id(self) -> int:
        """Gets most recent round_id from metadata table"""
        with Session(self.engine) as session:
            stmt = (
                select(metadata_table.c["round_id"]).order_by(
                    metadata_table.c["round_id"].desc()
                )
            ).limit(1)

            result = session.execute(stmt)
            
            round_id = result.fetchone()[0]

            self._log.info(f"Successfully pulled most recent round_id from database with value: {round_id}")

        return round_id