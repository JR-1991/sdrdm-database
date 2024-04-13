import os
import time
import rich
import asyncio
from enum import Enum
from itertools import cycle
from typing import Any, Dict, List, Optional, Union

import ibis
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from pydantic import BaseModel, ConfigDict, PrivateAttr, model_validator
from sdRDM import DataModel
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session

from sdrdm_database import commands
from sdrdm_database.modelutils import rebuild_api
from sdrdm_database.tablecreator import create_tables
from sdrdm_database.insert import map_multiple_to_sqlalchemy


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

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        use_enum_values=True,
    )

    db_name: str
    address: Optional[str] = None
    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    port: int = 5432
    address: Optional[str] = None
    dbtype: Union[str, SupportedBackends] = SupportedBackends.MYSQL
    connection: Optional[BaseAlchemyBackend] = None
    engine: Optional[Any] = None
    session: Optional[Any] = None

    _sqlalchemy_classes: Optional[Any] = PrivateAttr(None)
    _models: Dict[str, Any] = PrivateAttr({})
    _commands: Optional[commands.MetaCommands] = PrivateAttr(None)

    @model_validator(mode="after")  # type: ignore
    def _initial_setup(self) -> "DBConnector":
        """Performs initial setup for the DBConnector class."""

        if isinstance(self.dbtype, str):
            try:
                self.dbtype = SupportedBackends(self.dbtype)
            except ValueError:
                raise ValueError(
                    f"Invalid database type: {self.dbtype}. "
                    f"Supported types are: {SupportedBackends}"
                )

        self._commands = self._get_commands()

        if os.environ.get("TESTING_STAGE") == "unit_tests":
            return self

        self._connect()

        if "__model_meta__" in self.connection.list_tables():  # type: ignore
            self._build_models()

        print("🎉 Connected")

        return self

    def _connect(self):
        """Attempts to connect to the database using the appropriate connection method.

        Raises:
            ValueError: If the connection attempt fails.

        Returns:
            The database connection object.
        """

        try:
            self.connection, address = getattr(self, f"_connect_{self.dbtype.value}")()  # type: ignore

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
                self.connection.list_tables()  # type: ignore
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
        if "__model_meta__" not in self.connection.list_tables():  # type: ignore
            return

        model_meta = (
            self.connection.table("__model_meta__").execute().set_index("table")  # type: ignore
        )

        # Build root elements first
        root_models = model_meta[model_meta.part_of.isna()]
        root_libs = {}

        for root_name, row in root_models.iterrows():
            lib = rebuild_api(row.specifications, row.obj_name)
            root_libs[root_name] = lib

        # Build sub models
        sub_models = model_meta[model_meta.part_of.notna()]
        for sub_name, row in sub_models.iterrows():
            name = sub_name.split("_", 1)[-1]

            if sub_name not in sub_models.index:
                # Multiple primitive table
                continue

            self._models[sub_name] = getattr(root_libs[row.part_of], row.obj_name)
            self._models[row.obj_name] = getattr(root_libs[row.part_of], row.obj_name)
            self._models[name] = getattr(root_libs[row.part_of], row.obj_name)

    def _connect_engine(self, address: str):
        """Connect to the database using the SQLAlchemy engine."""
        self.engine = create_engine(address)

    def _create_address(self, backend: str, library: str):
        return f"{backend}+{library}://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    def _automap_classes(self):
        Base = automap_base()
        Base.prepare(
            autoload_with=self.engine,
            name_for_collection_relationship=self._name_for_collection_relationship,
            name_for_scalar_relationship=self._name_for_scalar_relationship,
        )
        self._sqlalchemy_classes = Base.classes

    @staticmethod
    def _name_for_collection_relationship(base, local_cls, referred_cls, constraint):
        join_table_name = constraint.table.name
        splitted = join_table_name.split("_")

        return "_".join(splitted[1:-1])

    @staticmethod
    def _name_for_scalar_relationship(base, local_cls, referred_cls, constraint):
        return constraint.name.replace("__fk", "")

    def _connect_duckdb(self):
        if self.address is None and self.dbtype == SupportedBackends.DUCKDB:  # type: ignore
            self.address = f"{self.dbtype.value}://{self.db_name}.ddb"  # type: ignore

        return ibis.connect(self.address)  # type: ignore

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
            return COMMAND_MAPPER[self.dbtype]  # type: ignore
        except KeyError:
            raise ValueError(
                f"Invalid database type: {self.dbtype}. "
                f"Supported types are: {COMMAND_MAPPER.keys()}"
            )

    # ! Table creation
    def create_tables(
        self,
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
                markdown_path=markdown_path,
            )

            # Build ORM related stuff
            self._build_models()
            self._automap_classes()

        except ConnectionRefusedError as e:
            print(
                "❌ Couldnt connect to database. Please check your credentials or status of the database."
            )

    # ! Getters and inserters
    def insert(
        self,
        *datasets: List[DataModel]
    ):
        """Inserts a dataset or multiple datasets into the database.

        Args:
            datasets (Union[DataModel, List[DataModel]]): The datasets to insert into the database.

        Returns:
            None
        """

        assert all(
            isinstance(dataset, DataModel) for dataset in datasets
        ), "All datasets must be of type DataModel."

        table_list = self.connection.list_tables() # type: ignore
        unknown_classes = [
            dataset.__class__.__name__ for dataset in datasets
            if dataset.__class__.__name__ not in table_list
        ]

        if unknown_classes:
            raise ValueError(
                f"❌ The following classes are not present in the database: {set(unknown_classes)}"
            )

        with self.start_session() as session:
            sql_objs = asyncio.run(
                map_multiple_to_sqlalchemy(
                    objs=datasets,
                    db=self,
                )
            )

            session.add_all(sql_objs)
            session.commit()

            rich.print(f"✅ Inserted {len(datasets)} rows into the database.")

    def get(
        self,
        model: Union[str, "DataModel"],  # type: ignore
        n_rows: Optional[int] = None,
    ) -> List["DataModel"]:  # type: ignore
        """
        Retrieves rows from the specified table that match the given criteria.

        Args:
            model (Union[str, DataModel]): The model to retrieve rows from.
        Returns:
            List[DataModel]: A list of DataModel objects that contain the retrieved rows.

        Raises:
            ValueError: If the table does not exist.
            AssertionError: If the table does not exist.
        """

        from sdRDM import DataModel

        if isinstance(model, str):
            table_name = model
        elif issubclass(model, DataModel): # type: ignore
            table_name = model.__name__
        else:
            raise ValueError("Model must be a string or DataModel object.")

        assert (
            table_name in self.connection.list_tables()  # type: ignore
        ), f"Table '{table_name}' does not exist."

        sql_model = getattr(self._sqlalchemy_classes, table_name)
        sdrdm_model = self._models[table_name]

        with self.start_session() as sess:

            if n_rows:
                results = sess.query(sql_model).limit(n_rows).all()
            else:
                results = sess.query(sql_model).all()

            return [sdrdm_model.model_validate(row) for row in results]

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

        if name not in self._models:
            raise ValueError(f"Requested model '{name}' is not registered.")

        return self._models[name]

    # ! Session manager
    def start_session(self):
        """Starts a new session with the database."""
        return Session(bind=self.engine)
