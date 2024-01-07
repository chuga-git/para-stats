from os import path, environ
from dotenv import load_dotenv

basedir = path.abspath(path.dirname(__name__))
load_dotenv(path.join(basedir, ".env"))


class Config:
    db_uri = environ.get("SQLALCHEMY_DB_URI")
    db_schema = environ.get("SQLALCHEMY_SCHEMA")
    db_mergetable = environ.get("SQLALCHEMY_MERGETABLE")
    db_temptable = environ.get("SQLALCHEMY_TEMPTABLE")
