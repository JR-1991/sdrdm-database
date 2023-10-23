from typing import Any, Callable, Dict, Optional

import ibis

from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from pydantic import BaseModel, PrivateAttr
from enum import Enum

from sdrdm_database import commands
from sdrdm_database.dataio import (
    _extract_related_rows,
    _query_equal,
    insert_into_database,
)
from sdrdm_database.modelutils import rebuild_api


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
        use_enum_values = True

    db_name: str
    address: Optional[str] = None
    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    port: int = 5432
    address: Optional[str] = None
    dbtype: SupportedBackends = SupportedBackends.POSTGRES
    connection: Optional[BaseAlchemyBackend] = None

    __commands__: Optional[commands.MetaCommands] = PrivateAttr(None)

    def __init__(self, **data) -> None:
        super().__init__(**data)

        if isinstance(self.dbtype, str):
            try:
                self.dbtype = SupportedBackends(self.dbtype)
            except ValueError:
                raise ValueError(
                    f"Invalid database type: {self.dbtype}. "
                    f"Supported types are: {SupportedBackends}"
                )

        self.__commands__ = self._get_commands()
        self._connect()

    def _connect(self):
        """Attempts to connect to the database using the appropriate connection method.

        Raises:
            ValueError: If the connection attempt fails.

        Returns:
            The database connection object.
        """

        try:
            self.connection = getattr(self, f"_connect_{self.dbtype.value}")()

        except Exception as e:
            raise ValueError(f"Could not connect to database: {e}") from e

    def _connect_duckdb(self):
        if self.address is None and self.dbtype == SupportedBackends.DUCKDB:
            self.address = f"{self.dbtype.value}://{self.db_name}.ddb"

        return ibis.connect(self.address)

    def _connect_postgres(self):
        assert self.username, "Username must be specified for Postgres"
        assert self.password, "Password must be specified for Postgres"
        assert self.host, "Host must be specified for Postgres"
        assert self.port, "Port must be specified for Postgres"
        assert self.db_name, "Database name must be specified for Postgres"

        return ibis.postgres.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.db_name,
        )

    def _connect_mysql(self):
        assert self.username, "Username must be specified for Postgres"
        assert self.password, "Password must be specified for Postgres"
        assert self.host, "Host must be specified for Postgres"
        assert self.port, "Port must be specified for Postgres"
        assert self.db_name, "Database name must be specified for Postgres"

        return ibis.mysql.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.db_name,
        )

    def _get_commands(self):
        """Returns the commands to use for the current database type.

        Returns:
            The commands to use for the current database type.
        """

        COMMAND_MAPPER = {
            SupportedBackends.POSTGRES: commands.PostgresCommands,
            SupportedBackends.MYSQL: commands.MySQLCommands,
        }

        try:
            return COMMAND_MAPPER[self.dbtype]
        except KeyError:
            raise ValueError(
                f"Invalid database type: {self.dbtype}. "
                f"Supported types are: {COMMAND_MAPPER.keys()}"
            )

    def insert(self, *datasets: "DataModel", verbose: bool = False):
        """Inserts data into the database.

        Args:
            table_name (str): The name of the table to insert the data into.
            data (dict): The data to insert into the database.
        """

        for dataset in datasets:
            try:
                insert_into_database(dataset=dataset, db=self)

                if verbose:
                    print(
                        f"Added dataset {dataset.__class__.__name__} ({str(dataset.__id__)})"
                    )
            except Exception as e:
                raise ValueError(f"Could not insert data into database: {e}") from e

    # ! Getters and inserters
    def get(
        self,
        table: str,
        attribute: Optional[str] = None,
        value: Optional[Any] = None,
        query_fun: Callable = _query_equal,
    ) -> "DataModel":
        """
        Retrieves rows from the specified table that match the given attribute and value.

        Args:
            table: The name of the table to retrieve rows from.
            attribute: The name of the attribute to match against.
            value: The value to match against the attribute.

        Returns:
            DataModel: A DataModel object that contains the retrieved rows.
        Raises:
            ValueError: If the requested model is not registered.
        """
        model_meta = (
            self.connection.table("__model_meta__").to_pandas().set_index("table")
        )

        if table not in model_meta.index:
            raise ValueError(f"Requested model '{table}' is not registered.")

        model = self.get_table_api(table)
        datasets = []

        table_ = self.connection.table(table)

        if query_fun and attribute and value:
            rows = table_[query_fun(table_, attribute, value)].execute()
        else:
            rows = table_.execute()

        for _, row in rows.iterrows():
            dataset = {}
            row_id = row[f"{table}_id"]
            _extract_related_rows(
                model=model,
                table_name=model.__name__,
                id_col=f"{model.__name__}_id",
                attr=f"{table}_id",
                target=row_id,
                query_fun=_query_equal,
                db_connector=self,
                dataset=dataset,
            )

            datasets.append(model(**dataset))

        return datasets

    # ! API Tools
    def get_table_api(self, name: str):
        """Returns an API for the specified table.

        Args:
            name (str): The name of the table.

        Returns:
            The API for the specified table.

        Raises:
            ValueError: If the requested model is not registered.
        """
        model_meta = (
            self.connection.table("__model_meta__").to_pandas().set_index("table")
        )

        if name not in model_meta.index:
            raise ValueError(f"Requested model '{name}' is not registered.")

        lib_specs = model_meta.loc[name].specifications
        obj_name = model_meta.loc[name].obj_name

        if lib_specs is None:
            part_of = model_meta.loc[name].part_of
            lib_specs = model_meta.loc[part_of].specifications

        return getattr(
            rebuild_api(lib_specs, obj_name),
            obj_name,
        )
