import ibis

from sdRDM import DataModel
from typing import Optional, List, Dict
from datetime import datetime, date
from functools import partial
from typing import get_origin
from pydantic import create_model

from sdrdm_database.modelutils import convert_md_to_json

from .dbconnector import DBConnector

TYPE_MAPPING = {
    str: "string",
    float: "float64",
    bool: "boolean",
    int: "int64",
    date: "string",
    datetime: "string",
}


def create_tables(
    db_connector: "DBConnector",
    model: "DataModel",
    markdown_path: str,
):
    """Creates tables according to the given sdRDM data model.

    Args:
        db_connector (DBConnector): Active Database connection to add tables to.
        model (DataModel): The model to create tables for.
    """

    _validate_input(db_connector=db_connector, model=model)

    print(f"\n🚀 Creating tables for data model {model.__name__}\n│")

    _add_to_model_table(
        table_name=model.__name__,
        db_connector=db_connector,
        markdown_path=markdown_path,
        obj_name=model.__name__,
    )

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

        if table_name in tables:
            print(f"├── Table '{table_name}'. Already exists in database. Skipping.")
            continue

        # Register the sub model
        if not instruction["is_primitive"]:
            _add_to_model_table(
                db_connector=db_connector,
                markdown_path=markdown_path,
                table_name=table_name,
                part_of=model.__name__,
                obj_name=instruction["obj_name"],
            )

        # Create the table
        db_connector.connection.create_table(
            table_name,
            schema=schema,
        )

        if instruction["is_primitive"] is False:
            instruction["pk_command"]()

        tables.append(table_name)

        print(f"├── Created table '{table_name}'")

        fk_commands += instruction["fk_commands"]

    for command in fk_commands:
        foreign_key = command.keywords["foreign_key"]
        reference_table = command.keywords["reference_table"]
        table_name = command.keywords["table_name"]
        table = db_connector.connection.table(table_name)

        if foreign_key in table.columns:
            print(
                f"├── Skipping foreign key '{foreign_key}'({reference_table}). Already exists in table {table_name}"
            )
            continue

        print(
            f"├── Added foreign key '{foreign_key}'({reference_table}) to table {table_name}"
        )
        command()

    print(f"│\n╰── 🎉 Created all tables for data model {model.__name__}\n")


def _validate_input(
    db_connector: "DBConnector",
    model: DataModel,
):
    assert db_connector.connection is not None, "No database connection established."
    assert issubclass(
        model, DataModel  # type: ignore
    ), f"Object {model} is not a subclass of DataModel and thus no valid sdRDM object. "

    try:
        db_connector.connection.list_tables()
    except Exception as e:
        raise Exception(f"Could not connect to database. Error: {e}")


def _add_to_model_table(
    db_connector: DBConnector,
    table_name: str,
    obj_name: str,
    markdown_path: str,
    github_url: Optional[str] = None,
    commit_hash: Optional[str] = None,
    part_of: Optional[str] = None,
):
    """Adds a table model to the __model_meta__ table in the database.

    Args:
        db_connector (DBConnector): The database connector object.
        table_name (str): The name of the table.
        obj_name (str): The name of the object.
        markdown_path (str): The path to the markdown file.
        github_url (Optional[str], optional): The URL of the GitHub repository. Defaults to None.
        commit_hash (Optional[str], optional): The commit hash of the repository. Defaults to None.
        part_of (Optional[str], optional): The name of the parent object. Defaults to None.
    """

    _create_model_meta_table(db_connector=db_connector)
    model_meta_table = db_connector.connection.table("__model_meta__").to_pandas()

    if not part_of:
        api_schema = convert_md_to_json(markdown_path)
    else:
        api_schema = None

    if table_name in model_meta_table["table"].values:
        print(f"├── Model '{table_name}' already registered. Skipping.")
        return

    db_connector.connection.insert(
        "__model_meta__",
        [
            {
                "table": table_name,
                "specifications": api_schema,
                "github_url": github_url,
                "commit_hash": commit_hash,
                "part_of": part_of,
                "obj_name": obj_name,
            }
        ],
    )

    print(f"├── Added table model '{table_name}' to __model_meta__ table")


def _create_model_meta_table(
    db_connector: DBConnector,
):
    """Creates a table named __model_meta__ in the database if it doesn't exist.

    Args:
        db_connector (DBConnector): A database connector object.
    """
    schema = ibis.schema(
        {
            "table": "!string",
            "specifications": "json",
            "github_url": "string",
            "commit_hash": "string",
            "part_of": "string",
            "obj_name": "string",
        }
    )

    if "__model_meta__" not in db_connector.connection.list_tables():
        print("├── Table __model_meta__ not existing. Adding to DB!")
        db_connector.connection.create_table(
            name="__model_meta__",
            schema=schema,
        )


def _create_table_schema(
    db_connector: DBConnector,
    data_model: "DataModel",
    table_name: str,
    schemes: List[Dict],
    parent: Optional[str] = None,
    is_primitive: bool = False,
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
        is_multiple = get_origin(attr.outer_type_) is list
        sub_table_name = f"{data_model.__name__}_{attr.name}"

        if attr.name == "id":
            continue
        elif is_multiple and not is_obj:
            _create_table_schema(
                db_connector=db_connector,
                data_model=create_model(
                    attr.name,
                    **{attr.name: (attr.type_, ...)},
                ),
                table_name=sub_table_name,
                schemes=schemes,
                parent=table_name,
                is_primitive=True,
            )
        elif is_obj:
            _create_table_schema(
                db_connector=db_connector,
                data_model=attr.type_,
                table_name=sub_table_name,
                schemes=schemes,
                parent=table_name,
            )
        else:
            _populate_schema(attr=attr, schema=schema)

    pk_fun = partial(
        db_connector.__commands__.add_primary_key,
        table_name=table_name,
        primary_key=f"{table_name}_id",
        con=db_connector.connection,
    )

    schemes.append(
        {
            "name": table_name,
            "obj_name": data_model.__name__,
            "schema": schema,
            "pk_command": pk_fun,
            "fk_commands": fk_commands,
            "is_primitive": is_primitive,
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
        "foreign_key": f"{parent}_id",
        "reference_table": parent,
        "reference_column": f"{parent}_id",
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
