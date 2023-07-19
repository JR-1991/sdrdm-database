import ibis

from sdRDM import DataModel
from typing import get_args, Optional, List, Dict
from datetime import datetime, date

from .dbconnector import DBConnector

TYPE_MAPPING = {
    str: "string",
    float: "float64",
    bool: "boolean",
    int: "int64",
    date: "string",
    datetime: "string",
}


def create_tables(db_connector: "DBConnector", model: DataModel):
    """Creates tables according to the given sdRDM data model.

    Args:
        db_connector (DBConnector): Active Database connection to add tables to.
        model (DataModel): The model to create tables for.
    """

    assert db_connector.connection is not None, "No database connection established."
    assert issubclass(
        model, DataModel  # type: ignore
    ), f"Object {model} is not a subclass of DataModel and thus no valid sdRDM object. "

    # Create schemes for each object found within the data model
    create_instructions = _create_table_schema(
        obj=model, table_name=model.__name__, schemes=[]  # type: ignore
    )

    # Create tables and add foreign keys
    fk_commands = []

    for instruction in create_instructions:
        table_name = instruction["name"]
        schema = ibis.schema(instruction["schema"])  # type: ignore

        # Create the table
        db_connector.connection.create_table(table_name, schema=schema, overwrite=True)

        # Add foreign key commands to run after table creation
        db_connector.connection.raw_sql(instruction["pk_command"])
        fk_commands += instruction["fk_commands"]

    for command in fk_commands:
        # Create all foreign keys
        db_connector.connection.raw_sql(command)


def _create_table_schema(
    obj: "DataModel", table_name: str, schemes: List[Dict], parent: Optional[str] = None
):
    schema, fk_commands = {}, []

    for name, field in obj.__fields__.items():
        if name == "id":
            continue

        is_optional = _is_optional(field.outer_type_)
        is_obj = hasattr(field.type_, "__fields__")

        if is_obj:
            _create_table_schema(field.type_, name, schemes, table_name)
            continue

        schema[name] = _map_type(field.type_, is_optional)

    if parent:
        schema[parent] = "int64"
        fk_commands.append(_add_foreign_key(table_name, parent, parent, "id"))

    schemes.append(
        {
            "name": table_name,
            "schema": schema,
            "pk_command": _add_autoincrement_primary_key(table_name),
            "fk_commands": fk_commands,
        }
    )

    return schemes


def _is_optional(dtype):
    return any(arg == type(None) for arg in get_args(dtype))


def _map_type(dtype, optional: bool) -> str:
    mapped_type = TYPE_MAPPING.get(dtype, "string")

    if not optional:
        return "!" + mapped_type

    return mapped_type


def _add_foreign_key(src_table: str, src_col: str, trgt_table: str, trgt_col: str):
    return f"""
        ALTER TABLE "{src_table}" ADD FOREIGN KEY ("{src_col}") REFERENCES "{trgt_table}"({trgt_col});
    """.strip()


def _add_autoincrement_primary_key(table: str):
    return f"""
        ALTER TABLE "{table}" ADD "id" INT PRIMARY KEY;
    """.strip()
