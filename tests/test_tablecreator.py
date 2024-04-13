from functools import partial
from typing import Optional
import pytest

from sdRDM import DataModel
from sdrdm_database.dbconnector import DBConnector
from sdrdm_database.tablecreator import (
    _create_table_schema,
    _map_type,
    _populate_schema,
)


class MockDataModel(DataModel):
    foo: str
    bar: Optional[int] = None

@pytest.mark.unit
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

# ADAPT TO ASYNC TESTS
# @pytest.mark.unit
# def test_populate_schema():
#     schema = {}

#     foo = MockDataModel.model_fields["foo"]
#     bar = MockDataModel.model_fields["bar"]

#     _populate_schema(field_info=foo, schema=schema)
#     _populate_schema(field_info=bar, schema=schema)

#     assert schema == {"foo": "!string", "bar": "int64"}

# ADAPT TO ASYNC TESTS
# @pytest.mark.unit
# def test_create_table_schema():
#     db_connector = DBConnector(
#         db_name="Test",
#         username="root",
#         password="root",
#         dbtype="mysql",
#         host="localhost",
#         port=3306,
#     )

#     data_model = MockDataModel
#     table_name = "table_name"
#     schemes = []

#     result = _create_table_schema(
#         db_connector=db_connector,
#         data_model=data_model, # type: ignore
#         table_name=table_name,
#     )

#     pk_command = result.pop("pk_command")
#     expected_pk = partial(
#         db_connector._commands.add_primary_key, # type: ignore
#         table_name=table_name,
#         primary_key=f"{table_name}_id",
#         dbconnector=db_connector,
#     )

#     expected_schema = {
#         "name": "table_name",
#         "schema": {"foo": "!string", "bar": "int64"},
#         "fk_commands": [],
#         "is_primitive": False,
#         "obj_name": "MockDataModel",
#     }

#     assert result == expected_schema
#     assert pk_command.keywords == expected_pk.keywords
#     assert pk_command.func == expected_pk.func
