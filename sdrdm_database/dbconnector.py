import os
import time
from enum import Enum
from itertools import cycle
from typing import Any, Dict, List, Optional

import ibis
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from pydantic import BaseModel, PrivateAttr
from bigtree import Node, find_child_by_name
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine

from sdrdm_database import commands
from sdrdm_database.dataio import _retrieve_documents, insert_into_database
from sdrdm_database.modelutils import rebuild_api
from sdrdm_database.tablecreator import create_tables
from sdrdm_database.treeutils import get_model_tree


class SupportedBackends(str, Enum):
    POSTGRES = "postgres"
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
    dbtype: SupportedBackends = SupportedBackends.MYSQL
    connection: Optional[BaseAlchemyBackend] = None
    engine: Optional[Any] = None

    __sqlalchemy_classes__: Optional[Any] = PrivateAttr(None)
    __relationships__: Dict[str, Node] = PrivateAttr({})
    __models__: Dict[str, Any] = PrivateAttr({})
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

        if os.environ.get("TESTING_STAGE") == "unit_tests":
            return

        self._connect()

        if "__model_meta__" in self.connection.list_tables():
            self._build_models()
            self._construct_relation_trees()

        print("🎉 Connected")

    def _connect(self):
        """Attempts to connect to the database using the appropriate connection method.

        Raises:
            ValueError: If the connection attempt fails.

        Returns:
            The database connection object.
        """

        try:
            self.connection, address = getattr(self, f"_connect_{self.dbtype.value}")()

        except Exception as e:
            raise ValueError(f"Could not connect to database: {e}") from e

        self._check_connection()
        self._connect_engine(address)
        self._automap_classes()

    def _check_connection(self):
        timeout = 60
        current_time = 0
        incr = 0.2

        connected = False
        animation = cycle(list("◐◓◑◒"))
        while not connected:
            try:
                self.connection.list_tables()
                connected = True
            except Exception as e:
                if current_time >= timeout:
                    raise ConnectionRefusedError(
                        f"Could not connect to database: {e}"
                    ) from e

                print(
                    f" {next(animation)} Waiting for database to be ready...",
                    end="\r",
                )
                time.sleep(incr)
                current_time += incr

        print(" " * 100, end="\r")

    def _build_models(self):
        if "__model_meta__" not in self.connection.list_tables():
            return

        model_meta = (
            self.connection.table("__model_meta__").execute().set_index("table")
        )

        # Build root elements first
        root_models = model_meta[model_meta.part_of.isna()]
        root_libs = {}

        for root_name, row in root_models.iterrows():
            lib = rebuild_api(row.specifications, row.obj_name)
            root_libs[root_name] = lib

            self.__models__[row.obj_name] = getattr(lib, row.obj_name)

        # Build sub models
        sub_models = model_meta[model_meta.part_of.notna()]
        for sub_name, row in sub_models.iterrows():
            name = sub_name.split("_", 1)[-1]

            if sub_name not in sub_models.index:
                # Multiple primitive table
                continue

            self.__models__[sub_name] = getattr(root_libs[row.part_of], row.obj_name)
            self.__models__[row.obj_name] = getattr(
                root_libs[row.part_of], row.obj_name
            )
            self.__models__[name] = getattr(root_libs[row.part_of], row.obj_name)

    def _construct_relation_trees(self):
        """Generate relationship trees for join queries."""

        model_meta = self.connection.table("__model_meta__").execute()
        roots = model_meta[model_meta.part_of.isna()]

        for name in roots.table:
            self.__relationships__[name] = get_model_tree(self, name)

        sub_trees = model_meta[model_meta.part_of.notna()]

        for _, row in sub_trees.iterrows():
            parent_tree = self.__relationships__[row.part_of]
            self.__relationships__[row.table] = find_child_by_name(
                parent_tree, row.table
            )

    def _connect_engine(self, address: str):
        """Connect to the database using the SQLAlchemy engine."""
        self.engine = create_engine(address)

    def _create_address(self, backend: str, library: str):
        return f"{backend}+{library}://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    def _automap_classes(self):
        Base = automap_base()
        Base.prepare(autoload_with=self.engine)
        self.__sqlalchemy_classes__ = Base.classes

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

        address = self._create_address("postgresql", "psycopg2")

        return (
            ibis.postgres.connect(
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.db_name,
            ),
            address,
        )

    def _connect_mysql(self):
        assert self.username, "Username must be specified for Postgres"
        assert self.password, "Password must be specified for Postgres"
        assert self.host, "Host must be specified for Postgres"
        assert self.port, "Port must be specified for Postgres"
        assert self.db_name, "Database name must be specified for Postgres"

        # Create engine
        address = self._create_address("mysql", "mysqlconnector")

        return (
            ibis.mysql.connect(
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.db_name,
            ),
            address,
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

            # Build ORM related stuff
            self._build_models()
            self._automap_classes()
            self._construct_relation_trees()
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
        criteria: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> List["DataModel"]:
        """
        Retrieves rows from the specified table that match the given criteria.

        Args:
            table_name (str): The name of the table to retrieve rows from.
            criteria (Optional[Dict[str, Dict[str, Any]]]): A dictionary of criteria to filter the rows by.
        Returns:
            List[DataModel]: A list of DataModel objects that contain the retrieved rows.

        Raises:
            ValueError: If the requested model is not registered.
            AssertionError: If the specified table does not exist.
        """

        assert (
            table_name in self.connection.list_tables()
        ), f"Table '{table_name}' does not exist."

        return _retrieve_documents(self, table_name, criteria)

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

        if name not in self.__models__:
            raise ValueError(f"Requested model '{name}' is not registered.")

        return self.__models__[name]
