import ibis

from sdRDM import DataModel
from typing import Optional, List, Dict
from datetime import datetime, date
from functools import partial

from .dbconnector import DBConnector

from pprint import pprint as print

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

    _validate_input(db_connector=db_connector, model=model)

    # Create schemes for each object found within the data model
    create_instructions = _create_table_schema(
        db_connector=db_connector,
        data_model=model,
        table_name=model.__name__,
        schemes=[],  # type: ignore
    )

    # Create tables and add foreign keys
    fk_commands = []
    tables = db_connector.connection.list_tables()

    for instruction in create_instructions[::-1]:
        table_name = instruction["name"]
        schema = ibis.schema(instruction["schema"])  # type: ignore

        if table_name not in tables:
            # Create the table
            db_connector.connection.create_table(
                table_name,
                schema=schema,
            )

            # Create primary key
            instruction["pk_command"]()
            tables.append(table_name)

        fk_commands += instruction["fk_commands"]

    for command in fk_commands:
        command()


def _validate_input(
    db_connector: "DBConnector",
    model: DataModel,
):
    assert db_connector.connection is not None, "No database connection established."
    assert issubclass(
        model, DataModel  # type: ignore
    ), f"Object {model} is not a subclass of DataModel and thus no valid sdRDM object. "


def _create_table_schema(
    db_connector: DBConnector,
    data_model: "DataModel",
    table_name: str,
    schemes: List[Dict],
    parent: Optional[str] = None,
):
    """Creates a table schema for a given DataModel object.

    Args:
        db_connector (DBConnector): A database connector object.
        obj (DataModel): A DataModel object.
        table_name (str): The name of the table to create.
        schemes (List[Dict]): A list of table schema dictionaries.
        parent (Optional[str], optional): The name of the parent table. Defaults to None.

    Returns:
        List[Dict]: A list of table schema dictionaries.
    """

    schema, fk_commands = {}, []

    _handle_foreign_keys(
        parent=parent,
        table_name=table_name,
        db_connector=db_connector,
        fk_commands=fk_commands,
    )

    for attr in data_model.__fields__.values():
        is_obj = hasattr(attr.type_, "__fields__")

        if attr.name == "id":
            continue
        elif is_obj:
            _create_table_schema(
                db_connector=db_connector,
                data_model=attr.type_,
                table_name=attr.name,
                schemes=schemes,
                parent=table_name,
            )
        else:
            _populate_schema(attr=attr, schema=schema)

    pk_fun = partial(
        db_connector.__commands__.add_primary_key,
        table_name=table_name,
        primary_key="id",
        con=db_connector.connection,
    )

    schemes.append(
        {
            "name": table_name,
            "schema": schema,
            "pk_command": pk_fun,
            "fk_commands": fk_commands,
        }
    )

    return schemes


def _handle_foreign_keys(
    parent: str,
    table_name: str,
    db_connector: DBConnector,
    fk_commands: List,
):
    """Add a foreign key to the table schema and create a command to add the foreign key to the database.

    Args:
        parent (str): The name of the parent table.
        table_name (str): The name of the table to add the foreign key to.
        schema (Dict): The schema of the table.
        db_connector (DBConnector): The database connector object.
        fk_commands (List): The list of commands to add foreign keys to the database.

    Returns:
        None
    """

    if not parent:
        return

    kwargs = {
        "table_name": table_name,
        "foreign_key": parent,
        "reference_table": parent,
        "reference_column": "id",
        "con": db_connector.connection,
    }

    fk_commands.append(
        partial(
            db_connector.__commands__.add_foreign_key,
            **kwargs,
        )
    )


def _populate_schema(
    attr,
    schema: Dict,
) -> None:
    """
    Populates the schema dictionary with the attribute name and its corresponding type.

    Args:
        attr: The attribute to add to the schema.
        schema: The schema dictionary to populate.
        table_name: The name of the table being created.
        schemes: A list of dictionaries representing the schema for each table.

    Returns:
        None
    """
    name = attr.name
    is_required = bool(attr.required)
    schema[name] = _map_type(
        attr.type_,
        is_required,
    )


def _map_type(
    dtype,
    required: bool,
) -> str:
    """
    Maps a given data type to its corresponding type within TYPE_MAPPING.

    Args:
        dtype: The data type to be mapped.
        required: A boolean indicating whether the field is required.

    Returns:
        The corresponding type string for the given data type.
    """
    mapped_type = TYPE_MAPPING.get(dtype, "NOT_SUPPORTED")

    if mapped_type == "NOT_SUPPORTED":
        raise ValueError(f"Type '{dtype}' is not supported yet.")

    if required:
        return "!" + mapped_type

    return mapped_type
