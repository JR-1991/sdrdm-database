from functools import partial
from typing import Optional
from pydantic import BaseModel
import pytest
from sdrdm_database.dbconnector import DBConnector
from sdrdm_database.tablecreator import (
    _create_table_schema,
    _map_type,
    _populate_schema,
    _handle_foreign_keys,
)


class MockDataModel(BaseModel):
    foo: str
    bar: Optional[int] = None


def test_map_type():
    # Test mapping of integer type
    assert _map_type(int, True) == "!int64", "Wrong mapping of mandatory integer type"
    assert _map_type(int, False) == "int64", "Wrong mapping of integer type"

    # Test mapping of float type
    assert _map_type(float, True) == "!float64", "Wrong mapping of mandatory float type"
    assert _map_type(float, False) == "float64", "Wrong mapping of float type"

    # Test mapping of string type
    assert _map_type(str, True) == "!string", "Wrong mapping of mandatory string type"
    assert _map_type(str, False) == "string", "Wrong mapping of string type"

    # Test mapping of boolean type
    assert _map_type(bool, True) == "!boolean", "Wrong mapping of mandatory bool type"
    assert _map_type(bool, False) == "boolean", "Wrong mapping of boolean type"

    # Test mapping of unsupported type
    with pytest.raises(ValueError):
        _map_type(list, True)


def test_populate_schema():
    class MockDataModel(BaseModel):
        foo: str
        bar: Optional[int] = None

    schema = {}

    foo = MockDataModel.model_fields["foo"]
    bar = MockDataModel.model_fields["bar"]

    _populate_schema(attr=foo, schema=schema)
    _populate_schema(attr=bar, schema=schema)

    assert schema == {"foo": "!string", "bar": "int64"}


def test_handle_foreign_keys():
    db_connector = DBConnector(
        db_name="Test",
        username="root",
        password="root",
        dbtype="mysql",
        host="localhost",
        port=3306,
    )

    parent = "parent"
    table_name = "table_name"
    fk_commands = []

    _handle_foreign_keys(
        parent=parent,
        table_name=table_name,
        db_connector=db_connector,
        fk_commands=fk_commands,
    )

    expected_kwargs = {
        "table_name": table_name,
        "foreign_key": f"{parent}_id",
        "reference_table": "parent",
        "reference_column": f"{parent}_id",
        "dbconnector": db_connector,
    }

    assert fk_commands[0].keywords == expected_kwargs
    assert fk_commands[0].func == db_connector._commands.add_foreign_key


def test_create_table_schema():
    db_connector = DBConnector(
        db_name="Test",
        username="root",
        password="root",
        dbtype="mysql",
        host="localhost",
        port=3306,
    )

    data_model = MockDataModel
    table_name = "table_name"
    schemes = []

    result = _create_table_schema(
        db_connector=db_connector,
        data_model=data_model,
        table_name=table_name,
        schemes=schemes,
    )

    pk_command = result[0].pop("pk_command")
    expected_pk = partial(
        db_connector._commands.add_primary_key,
        table_name=table_name,
        primary_key=f"{table_name}_id",
        dbconnector=db_connector,
    )

    expected_schema = {
        "name": "table_name",
        "schema": {"foo": "!string", "bar": "int64"},
        "fk_commands": [],
        "is_primitive": False,
        "obj_name": "MockDataModel",
    }

    assert result == [expected_schema]
    assert pk_command.keywords == expected_pk.keywords
    assert pk_command.func == expected_pk.func
