from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import Session
from sqlalchemy.types import Integer, Text
from sqlalchemy.dialects.postgresql import JSONB, insert
from .models import round_table


class DatabaseLoader:
    def __init__(self, config) -> None:
        self.db_uri = config.db_uri
        self.db_ods_schema = config.db_ods_schema
        self.ods_table = config.db_ods_table

        self.engine = create_engine(self.db_uri, echo=False)
        self.db_metadata = MetaData(schema=self.db_ods_schema) # the models.py metadata overwrites this one which means that the schema isn't saved. genius.
        self.db_metadata.create_all(bind=self.engine, tables=[round_table])

    # this doesnt work because you cant FUCKING UPSERT
    def __upload_dataframe(self, conn, df, table):
        df.to_sql(
            name=table,
            con=conn,
            if_exists="replace",
            chunksize=500,
            index=False,
            schema=self.db_ods_schema,
            dtype={
                "round_id": Integer,
                "init_datetime": Text,
                "start_datetime": Text,
                "shutdown_datetime": Text,
                "end_datetime": Text,
                "commit_hash": Text,
                "game_mode": Text,
                "game_mode_result": Text,
                "end_state": Text,
                "map_name": Text,
                "server_id": Text,
                "playercounts": JSONB,
                "stats": JSONB,
            },
        )

        # fuck my life
        conn.execute(text("""ALTER "rounds_temp" ADD PRIMARY KEY round_id;"""))

        return f"Uploaded {len(df)} rows to {table}"

    def upload_round_list(self, round_list: list) -> str:
        """
        Loads cleaned round list into postgres database and returns success string
        TODO: this needs to upsert instead of do nothing on conflict because something is going to break at some point and it needs to be overwritten
        """

        # why does it need a session AND a session.commit() to work? i don't know!!!
        with Session(self.engine) as session:
            session.execute(insert(round_table).on_conflict_do_nothing(), round_list)
            session.commit()

        # TODO: figure out how to get the reflected Result rowcount lmfao
        result_statement = f"Inserted {len(round_list)} rows into {self.ods_table}"

        return result_statement
