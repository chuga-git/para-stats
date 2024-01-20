from config import Config
from sqlalchemy import Column, Table, MetaData
from sqlalchemy.types import Integer, Text
from sqlalchemy.dialects.postgresql import JSONB

"""
https://blog.codinghorror.com/object-relational-mapping-is-the-vietnam-of-computer-science/
"""

db_metadata = MetaData(schema=Config.db_ods_schema)

round_table = Table(
    Config.db_ods_rounds_table,
    db_metadata,
    Column("round_id", Integer, primary_key=True),
    Column("init_datetime", Text),
    Column("start_datetime", Text),
    Column("shutdown_datetime", Text),
    Column("end_datetime", Text),
    Column("commit_hash", Text),
    Column("game_mode", Text),
    Column("game_mode_result", Text),
    Column("end_state", Text),
    Column("map_name", Text),
    Column("server_id", Text),
    Column("playercounts", JSONB),
    Column("stats", JSONB)
)

metadata_table = Table(
    Config.db_ods_metadata_table,
    db_metadata,
    Column("round_id", Integer, primary_key=True),
    Column("init_datetime", Text),
    Column("start_datetime", Text),
    Column("shutdown_datetime", Text),
    Column("end_datetime", Text),
    Column("commit_hash", Text),
    Column("game_mode", Text),
    Column("game_mode_result", Text),
    Column("end_state", Text),
    Column("map_name", Text),
    Column("server_id", Text),
)