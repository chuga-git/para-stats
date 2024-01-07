from config import Config
from sqlalchemy import Column, Table, MetaData
from sqlalchemy.types import Integer, Text
from sqlalchemy.dialects.postgresql import JSONB

"""
https://blog.codinghorror.com/object-relational-mapping-is-the-vietnam-of-computer-science/
"""

meta = MetaData()

round_table = Table(
    Config.db_mergetable,
    meta,
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


# class Round(Base):
#     __tablename__ = 'rounds'

#     round_id: Mapped[int] = mapped_column(Integer, primary_key=True)
#     init_datetime: Mapped[str] = mapped_column(Text)
#     start_datetime: Mapped[str] = mapped_column(Text)
#     shutdown_datetime: Mapped[str] = mapped_column(Text)
#     end_datetime: Mapped[str] = mapped_column(Text)
#     commit_hash: Mapped[str] = mapped_column(Text)
#     game_mode: Mapped[str] = mapped_column(Text)
#     game_mode_result: Mapped[str] = mapped_column(Text)
#     end_state: Mapped[str] = mapped_column(Text)
#     map_name: Mapped[str] = mapped_column(Text)
#     server_id: Mapped[str] = mapped_column(Text)
#     timestamps: Mapped[Dict] = mapped_column(JSONB)
#     stats: Mapped[List[Dict]] = mapped_column(JSONB)
