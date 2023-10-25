from typing import Any, Callable, Dict, List, Optional, Union

import ibis

from ibis.expr.types.relations import Table
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from pydantic import BaseModel, PrivateAttr
from enum import Enum

from sdrdm_database import commands
from sdrdm_database.dataio import (
    _extract_related_rows,
    insert_into_database,
)
from sdrdm_database.modelutils import rebuild_api
from sdrdm_database.tablecreator import create_tables


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

    # ! Table creation
    def create_tables(
        self,
        model: "DataModel",
        markdown_path: str,
    ):
        """Creates tables in the database from a DataModel.

        Args:
            model (DataModel): The DataModel to create tables from.
            markdown_path (str): The path/GitURL to the markdown file that contains the DataModel.
        """

        try:
            create_tables(
                db_connector=self,
                model=model,
                markdown_path=markdown_path,
            )
        except ConnectionRefusedError as e:
            print(
                "❌ Couldnt connect to database. Please check your credentials or status of the database."
            )

    # ! Getters and inserters
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

    def get(
        self,
        table_name: str,
        filtered_table: Optional[Table] = None,
        max_rows: int = 10,
        model: Optional["DataModel"] = None,
    ) -> List["DataModel"]:
        """
        Retrieves rows from the specified table that match the given attribute and value.

        Args:
            table_name (str): The name of the table to retrieve rows from.
            filtered_table (Optional[Table], optional): A filtered table. Defaults to None.
            max_rows (int, optional): The maximum number of rows to retrieve. Defaults to 10.

        Returns:
            List[DataModel]: A list of DataModel objects that contain the retrieved rows.

        Raises:
            ValueError: If the requested model is not registered.
        """

        if filtered_table is not None:
            table = filtered_table
        else:
            table = self.connection.table(table_name)

        if model is None:
            model = self.get_table_api(table_name)

        datasets = _extract_related_rows(
            table=table,
            id_col=f"{table_name}_id",
            db=self,
            model=model,
            MAX_ROWS=max_rows,
        )

        return [model(**d) for d in datasets]

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
