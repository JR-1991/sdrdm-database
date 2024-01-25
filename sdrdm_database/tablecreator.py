from enum import Enum
import json
import ibis
import numpy
import validators

from sdRDM import DataModel
from typing import Optional, Dict, get_args
from datetime import datetime, date
from functools import partial
from typing import get_origin
from pydantic import PositiveFloat, PositiveInt, StrictBool

from sdrdm_database.modelutils import (
    convert_md_to_json,
    extract_lib_relations,
    get_md_content,
)


TYPE_MAPPING = {
    str: "string",
    float: "float64",
    bool: "boolean",
    int: "int64",
    date: "string",
    datetime: "string",
    bytes: "bytes",
    StrictBool: "boolean",
    PositiveFloat: "float64",
    PositiveInt: "int64",
    numpy.ndarray: "bytes",
}


def create_tables(db_connector: "DBConnector", markdown_path: str):
    """Creates tables according to the given sdRDM data model.

    Args:
        db_connector (DBConnector): Active Database connection to add tables to.
        model (DataModel): The model to create tables for.
    """

    _validate_input(db_connector=db_connector)

    print(f"\n🚀 Creating tables for data model {markdown_path}\n│")

    if validators.url(markdown_path):
        lib = DataModel.from_git(markdown_path)
    else:
        lib = DataModel.from_markdown(markdown_path)

    relations = extract_lib_relations(lib)

    for table, fields in relations.items():
        if isinstance(fields, dict):
            names = []
            types = []
            for field, data in fields.items():
                names.append(field)
                type = data.get("type")
                types.append(type)
                #references = data.get('references')
                #print(field,type, references)
                #todo: create references
        schema = ibis.schema(names=names, types= types)
        db_connector.connection.create_table(table, schema=ibis.schema(names=names, types= types) )

    md_content = get_md_content(markdown_path)
    root_name = "DATA_MODEL"
    _add_to_model_table(
        table_name=root_name,
        db_connector=db_connector,
        md_content=md_content,
        obj_name=root_name,
    )

    # Create schemes for each object found within the data model
    instructions = []
    for obj in lib.__dict__.values():
        if not hasattr(obj, "__fields__"):
            continue
       
        table_name = obj.__name__
        instructions.append(
            _create_table_schema(
                db_connector=db_connector,
                data_model=obj,
                table_name=table_name,
            )
        )

        _add_to_model_table(
            table_name=table_name,
            db_connector=db_connector,
            md_content=md_content,
            obj_name=table_name,
            part_of=root_name,
        )

    tables = db_connector.connection.list_tables()
    pk_commands = []

    for instruction in instructions:
        table_name = instruction["name"]
        schema = ibis.schema(instruction["schema"])  # type: ignore

        if table_name in tables:
            print(f"├── Table '{table_name}'. Already exists in database. Skipping.")
            continue

        # Create the table
        db_connector.connection.create_table(table_name, schema=schema)
        pk_commands.append(instruction["pk_command"])
        tables.append(table_name)

        print(f"├── Created table '{table_name}'")

    for command in pk_commands:
        primary_key = command.keywords["primary_key"]
        table_name = command.keywords["table_name"]
        command()

        print(f"├── Added primary key '{primary_key}' to table {table_name}")

    db_connector._build_models()

    print(f"│\n╰── 🎉 Created all tables for data model {markdown_path}\n")


def _validate_input(db_connector: "DBConnector"):
    """
    Validates the input parameters for the _validate_input function.

    Args:
        db_connector (DBConnector): The database connector object.

    Raises:
        Exception: If no database connection is established or if there is an error connecting to the database.
    """
    assert db_connector.connection is not None, "No database connection established."

    try:
        db_connector.connection.list_tables()
    except Exception as e:
        raise Exception(f"Could not connect to database. Error: {e}")


def _add_to_model_table(
    db_connector: "DBConnector",
    table_name: str,
    obj_name: str,
    md_content: str,
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
        api_schema = convert_md_to_json(md_content)
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


def _create_model_meta_table(db_connector: "DBConnector"):
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
    db_connector: "DBConnector",
    data_model: "DataModel",
    table_name: str,
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

    schema = {}

    for attr in data_model.__fields__.values():
        is_obj = hasattr(attr.type_, "__fields__")
        is_multiple = get_origin(attr.outer_type_) is list

        if attr.name == "id":
            continue

        if is_obj and not is_multiple:
            continue

        if is_multiple:
            pass
            #schema[attr] = "string"

        else:
            _populate_schema(attr=attr, schema=schema)

    pk_fun = partial(
        db_connector.__commands__.add_primary_key,
        table_name=table_name,
        primary_key="id",
        dbconnector=db_connector,
    )

    return {
        "name": table_name,
        "obj_name": data_model.__name__,
        "schema": schema,
        "pk_command": pk_fun,
    }


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

    if get_args(dtype):
        dtype = _deconstruct_union_type(dtype)
    if issubclass(dtype, Enum):
        dtype = _get_enum_type(dtype)

    mapped_type = TYPE_MAPPING.get(dtype, "NOT_SUPPORTED")

    if "ConstrainedStrValue" in repr(dtype):
        mapped_type = "string"

    if mapped_type == "NOT_SUPPORTED":
        raise ValueError(f"Type '{dtype}' is not supported yet.")

    if required:
        return "!" + mapped_type

    return mapped_type


def _deconstruct_union_type(dtype):
    """Deconstructs a union type into its primitive types.

    Args:
        dtype: The data type to deconstruct.

    Returns:
        A list of primitive types.
    """
    types = [
        dt
        for dt in get_args(dtype)
        if not hasattr(dt, "__fields__") and not dt == type(None)
    ]

    if len(types) > 1:
        raise ValueError(f"Multiple types '{dtype}' are not supported yet.")

    return types[0]


def _get_enum_type(enum):
    dtypes = list({type(member.value) for member in enum.__members__.values()})

    if len(dtypes) > 1:
        raise ValueError(f"Multiple types '{dtypes}' are not supported yet.")

    return dtypes[0]
