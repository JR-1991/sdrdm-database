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
        con: "BaseAlchemyBackend",
    ):
        pass


class MySQLCommands(MetaCommands):
    @staticmethod
    def add_primary_key(
        table_name: str,
        primary_key: str,
        con: "BaseAlchemyBackend",
    ):
        try:
            con.raw_sql(
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
        con: "BaseAlchemyBackend",
    ):
        try:
            con.raw_sql(
                f"ALTER TABLE {table_name} ADD COLUMN {foreign_key} VARCHAR(36);"
            )
            con.raw_sql(
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
        con: "BaseAlchemyBackend",
    ):
        try:
            con.raw_sql(
                f"ALTER TABLE {table_name} ADD COLUMN {primary_key} UUID PRIMARY KEY;"
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
        con: "BaseAlchemyBackend",
    ):
        try:
            con.raw_sql(f"ALTER TABLE {table_name} ADD COLUMN {foreign_key} UUID;")
            con.raw_sql(
                f"ALTER TABLE {table_name} ADD FOREIGN KEY ({foreign_key}) REFERENCES {reference_table}({reference_column});"
            )
        except Exception as e:
            print(
                f"Could not add foreign key {foreign_key} for table {table_name} to {reference_table}({reference_column}): "
            )
            raise e
