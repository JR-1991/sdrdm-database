from enum import Enum
import strawberry

from sdrdm_database import DBConnector
from typing import Any, Callable, Optional, Tuple, get_args, get_origin, List
from pydantic import create_model

from sdrdm_database.dbconnector import SupportedBackends
from sdrdm_database.tablecreator import _deconstruct_union_type


from typing import Tuple, List, Any, Optional, Callable
import strawberry


def prepare_graphql(
    table: str,
    username: str,
    password: str,
    port: int,
    db_name: str,
    host: str,
    dbtype: SupportedBackends,
) -> Tuple[strawberry.type, Callable[[Optional[str]], List[Any]]]:
    """
    Prepares a GraphQL schema and resolver function for a given database table.

    Args:
        table (str): The name of the database table to create a schema for.
        username (str): The username to use when connecting to the database.
        password (str): The password to use when connecting to the database.
        port (int): The port number to use when connecting to the database.
        db_name (str): The name of the database to connect to.
        host (str): The hostname or IP address of the database server.
        dbtype (SupportedBackends): The type of database backend to use.

    Returns:
        Tuple[strawberry.type, Callable[[Optional[str]], List[Any]]]: A tuple containing the GraphQL schema type and a resolver function for the schema.
    """

    db = DBConnector(
        username=username,
        password=password,
        host=host,
        db_name=db_name,
        port=port,
        dbtype=dbtype,
    )

    model_registry = {}
    model = db.get_table_api(table)
    dtype = _convert_model(model, model_registry)

    for name, submodel in model_registry.items():
        schema_type = strawberry.experimental.pydantic.type(
            model=submodel, all_fields=True
        )(type(name + "Type", (), {}))

        model_registry[name] = schema_type

    def _resolve(id: Optional[str] = None):
        return _resolver_fun(
            db=db,
            table_name=table,
            dtype=model_registry[table],
            id=id,
            model=dtype,
        )

    return model_registry, _resolve


def _convert_model(model, registered_models={}):
    """
    Converts a Pydantic model to a Strawberry type.

    Args:
        model (pydantic.BaseModel): The Pydantic model to convert.

    Returns:
        strawberry.type: The converted Strawberry type.
    """
    to_model = {}
    for attr in model.__fields__.values():
        dtype = _prepare_dtype(attr, registered_models)
        to_model[attr.name] = (dtype, ...)

    converted = create_model(model.__name__, **to_model)
    registered_models[model.__name__] = converted

    return converted


def _prepare_dtype(
    attr,
    registered_models,
):
    is_multiple = get_origin(attr.outer_type_) is list
    is_obj = hasattr(attr.type_, "__fields__")
    dtype = attr.type_

    if get_args(attr.type_):
        dtype = _deconstruct_union_type(dtype)
    elif issubclass(attr.type_, Enum):
        dtype = str

    if is_obj and not dtype.__name__ in registered_models:
        dtype = _convert_model(
            attr.type_,
            registered_models,
        )
    elif is_obj and dtype.__name__ in registered_models:
        dtype = registered_models[dtype.__name__]

    if dtype == bytes:
        dtype = str

    if is_multiple:
        return List[dtype]
    else:
        return dtype


def _resolver_fun(
    db: "DBConnector",
    table_name: str,
    dtype: strawberry.type,
    model: Any = None,
    id=None,
):
    """
    Resolves a GraphQL query by fetching data from the database.

    Args:
        db (DBConnector): The database connector object.
        table_name (str): The name of the table to fetch data from.
        dtype (strawberry.type): The GraphQL type to convert the data to.
        id (int, optional): The ID of the row to fetch. Defaults to None.

    Returns:
        List[dtype]: A list of objects of the specified GraphQL type.
    """

    if id is not None:
        table = db.connection.table(table_name)
        filtered = table[table[f"{table_name}_id"] == id]
        result = db.get(
            table_name=table_name,
            filtered_table=filtered,
        )
    else:
        result = db.get(table_name)

    return [dtype.from_pydantic(row) for row in result]
