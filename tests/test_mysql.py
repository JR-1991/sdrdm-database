from copy import deepcopy
import pytest

from sdRDM import DataModel
from sdRDM.base.listplus import ListPlus


def sort_subkeys(data):
    """
    Sorts sub keys that are lists in a dictionary recursively.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (list, ListPlus)):
                is_complex = all(isinstance(v, dict) for v in value)
            else:
                is_complex = isinstance(value, dict)

            if isinstance(value, list) and is_complex:
                data[key] = sorted(value, key=lambda x: x["name"])
            elif isinstance(value, list) and not is_complex:
                data[key] = sorted(value)
            elif isinstance(value, dict):
                data[key] = sort_subkeys(value)
    return data


@pytest.mark.integration
def test_mysql():
    from sdrdm_database import DBConnector

    # Establish a connection to the database
    db = DBConnector(
        username="root",
        password="root",
        host="localhost",
        db_name="db",
        port=3306,
        dbtype="mysql",
    )

    assert db.connection is not None, "Connection not established"

    # Load model
    lib = DataModel.from_markdown("./.github/integration/model.md")
    db.create_tables(markdown_path="./.github/integration/model.md")

    # Check tables
    expected_tables = set(
        [
            "Test",
            "Test_nested_Nested",
            "__model_meta__",
        ]
    )

    assert (
        set(db.connection.list_tables()) == expected_tables
    ), f"Expected tables '{expected_tables}' but got '{db.connection.list_tables()}'"

    # Check schema
    table = db.connection.table("Test")
    schema = {
        key: ("!" if not dtype.nullable else "") + dtype.__class__.__name__.lower()
        for key, dtype in table.schema().fields.items()
    }

    expected = {
        "id": "!string",
        "name": "string",
        "int_value": "int64",
        "float_value": "float64",
        "bool_value": "boolean",
    }

    expected_repeat = deepcopy(expected)
    expected_repeat["bool_value"] = "int8"

    assert (
        schema == expected or schema == expected_repeat
    ), f"Expected schema '{expected}' but got '{schema}'"

    # Add data
    obj = lib.Test(
        int_value=1,
        float_value=1.0,
        bool_value=True,
        name="Hello",
    )

    obj.add_to_nested(name="Hello")

    db.insert(obj)

    expected = {**obj.dict(exclude_unset=True), "id": str(obj._id)}
    table = db.connection.table("Test").execute()
    entry = table[table.id == str(obj._id)].iloc[0].to_dict()

    assert entry == expected, f"Expected entry '{expected}' but got '{entry}'"

    # Retrieve the object again
    retrieved = db.get("Test")[0]
    to_exclude = {
        "id": True,
        "nested": {
            0: {"id": True},
        },
    }

    expected = obj.dict(exclude=to_exclude)
    retrieved = retrieved.dict(exclude=to_exclude)

    assert expected == retrieved, f"Expected object '{obj}' but got '{retrieved}'"
