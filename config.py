from os import path, environ
from dotenv import load_dotenv

basedir = path.abspath(path.dirname(__name__))
load_dotenv(path.join(basedir, ".env"))


class Config:
    db_uri = environ.get("SQLALCHEMY_DB_URI")
    db_ods_schema = environ.get("SQLALCHEMY_ODS_SCHEMA")
    db_ods_rounds_table = environ.get("SQLALCHEMY_ODS_ROUNDS_TABLE")
    db_ods_metadata_Table = environ.get("SQLALCHEMY_ODS_METADATA_TABLE")