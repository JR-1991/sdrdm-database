import psycopg2
from abc import ABC, abstractmethod


class MetaCommands(ABC):
    @abstractmethod
    def add_primary_key(
        table_name: str,
        primary_key: str,
        con: "BaseAlchemyBackend",
    ):
        pass

    @abstractmethod
    def add_foreign_key(
        table_name: str,
        foreign_key: str,
        reference_table: str,
        reference_column: str,
        dbconnector: "DBConnector",
    ):
        pass


class MySQLCommands(MetaCommands):
    @staticmethod
    def add_primary_key(
        table_name: str,
        primary_key: str,
        dbconnector: "BaseAlchemyBackend",
    ):
        try:
            dbconnector.connection.raw_sql(
                f"ALTER TABLE {table_name} ADD COLUMN {primary_key} VARCHAR(36) PRIMARY KEY;"
            )
        except Exception as e:
            print(f"Could not add primary key {primary_key} for table {table_name}: ")
            raise e

    @staticmethod
    def add_foreign_key(
        table_name: str,
        foreign_key: str,
        reference_table: str,
        reference_column: str,
        dbconnector: "DBConnector",
    ):
        try:
            dbconnector.connection.raw_sql(
                f"ALTER TABLE {table_name} ADD COLUMN {foreign_key} VARCHAR(36);"
            )
            dbconnector.connection.raw_sql(
                f"ALTER TABLE {table_name} ADD FOREIGN KEY ({foreign_key}) REFERENCES {reference_table}({reference_column});"
            )
        except Exception as e:
            print(
                f"Could not add foreign key {foreign_key} for table {table_name} to {reference_table}({reference_column}): "
            )
            raise e


class PostgresCommands(MetaCommands):
    @staticmethod
    def add_primary_key(
        table_name: str,
        primary_key: str,
        dbconnector: "DBConnector",
    ):
        try:
            with psycopg2.connect(
                dbname=dbconnector.db_name,
                user=dbconnector.username,
                password=dbconnector.password,
                host=dbconnector.host,
                port=dbconnector.port,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f'ALTER TABLE "{table_name}" ADD COLUMN "{primary_key}" VARCHAR(36);'
                    )
                    cur.execute(
                        f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("{primary_key}");'
                    )
        except Exception as e:
            print(f"Could not add primary key {primary_key} for table {table_name}: ")
            raise e

    @staticmethod
    def add_foreign_key(
        table_name: str,
        foreign_key: str,
        reference_table: str,
        reference_column: str,
        dbconnector: "BaseAlchemyBackend",
    ):
        try:
            with psycopg2.connect(
                dbname=dbconnector.db_name,
                user=dbconnector.username,
                password=dbconnector.password,
                host=dbconnector.host,
                port=dbconnector.port,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f'ALTER TABLE "{table_name}" ADD COLUMN "{foreign_key}" VARCHAR(36);'
                    )
                    cur.execute(
                        f'ALTER TABLE "{table_name}" ADD FOREIGN KEY ("{foreign_key}") REFERENCES "{reference_table}"("{reference_column}");'
                    )
        except Exception as e:
            print(
                f"Could not add foreign key {foreign_key} for table {table_name} to {reference_table}({reference_column}): "
            )
            raise e
