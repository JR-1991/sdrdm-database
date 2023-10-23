import strawberry
import toml
import typer

from strawberry.schema.config import StrawberryConfig
from sdrdm_database import DBConnector
from typing import Any, get_origin, List
from pydantic import create_model

from sdrdm_database.dbconnector import SupportedBackends


def fetch_model(
    table: str,
    db: DBConnector,
):
    if "__model_meta__" not in db.connection.list_tables():
        raise ValueError("The database does not contain a table for the model")

    return db.get_table_api(table)


def connect_to_database(
    username: str,
    password: str,
    port: int,
    db_name: str,
    dbtype: SupportedBackends,
    host: str = "localhost",
):
    return DBConnector(
        username=username,
        password=password,
        host=host,
        db_name=db_name,
        port=port,
        dbtype=dbtype,
    )


def convert_model(model):
    to_model = {}
    for attr in model.__fields__.values():
        dtype = attr.type_
        is_obj = hasattr(attr.type_, "__fields__")
        is_multiple = get_origin(attr.outer_type_) is list

        if is_obj:
            dtype = convert_model(attr.type_)

        if is_multiple:
            to_model[attr.name] = (List[dtype], ...)
        else:
            to_model[attr.name] = (attr.type_, ...)

    converted = create_model(model.__name__, **to_model)

    return strawberry.experimental.pydantic.type(model=converted, all_fields=True)(
        type(converted.__name__, (), {})
    )


def resolver_fun(db, model, dtype):
    result = db.get(model.__name__)
    return [dtype.from_pydantic(row) for row in result]


with open("env.toml") as f:
    env = toml.load(f)

    table = env.get("table")
    username = env.get("username")
    password = env.get("password")
    port = env.get("port")
    db_name = env.get("db_name")
    host = env.get("host")
    dbtype = env.get("dbtype")


db = connect_to_database(
    username=username,
    password=password,
    port=port,
    db_name=db_name,
    host=host,
    dbtype=dbtype,
)

model = fetch_model(table, db)
dtype = convert_model(model)


@strawberry.type
class Query:
    roots: List[dtype] = strawberry.field(
        resolver=lambda: resolver_fun(db=db, model=model, dtype=dtype)
    )


schema = strawberry.Schema(
    query=Query,
    config=StrawberryConfig(auto_camel_case=False),
)
