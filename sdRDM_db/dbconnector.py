from typing import Optional

import ibis

from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from pydantic import BaseModel
from enum import Enum


class SupportedBackends(str, Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    DUCKDB = "duckdb"
    MYSQL = "mysql"


class DBConnector(BaseModel):
    """
    Class to connect to a database and store the connection. This class can
    also be used to insert and retrieve data from the database. For more
    information on how to use this class, see the Ibis documentation.

    Example:

        (1) Establish a connection and list all tables

        >>> from sdRDM_db import DBConnector
        >>> db = DBConnector(user="postgres", password="postgres", host="localhost", db_name="postgres")
        >>> db.con.list_tables()

        >>> ["table1", "table2", ...]

        (2) Create a table from a model

        >>> from sdRDM_db import DBConnector, create_tables
        >>> from sdRDM import DataModel

        >>> db = DBConnector(user="postgres", password="postgres", host="localhost", db_name="postgres")
        >>> model = DataModel.from_git(...)
        >>> create_tables(db_connector=db, model=model)

        >>> db.con.list_tables()
        >>> ["table1", "table2", ...]

    For more information on how to use Ibis, see the Ibis documentation:

    https://ibis-project.org/docs

    """

    class Config:
        arbitrary_types_allowed = True

    host: str
    username: str
    password: str
    db_name: str
    port: int = 5432
    dbtype: SupportedBackends = SupportedBackends.POSTGRES
    connection: Optional[BaseAlchemyBackend] = None

    def __init__(self, **data) -> None:
        super().__init__(**data)

        # Connect to the database
        self._connect()

    def _connect(self):
        """Establoish a connection to the database."""

        # Get the type of DB and Ibis backend
        backend_module = self._get_backend_module()

        # Try to connect to the database
        try:
            self.connection = backend_module.connect(
                user=self.username,
                password=self.password,
                port=self.port,
                database=self.db_name,
            )

        except Exception as e:
            raise ValueError(f"Could not connect to database: {e}")

    def _get_backend_module(self):
        try:
            return getattr(ibis, self.dbtype.value)
        except AttributeError:
            raise ValueError(f"Backend {self.dbtype.value} not supported")
