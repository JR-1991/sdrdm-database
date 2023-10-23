import pytest

from sdRDM import DataModel
from sdRDM.base.listplus import ListPlus
from sdrdm_database import create_tables


def sort_subkeys(data):
    """
    Sorts sub keys that are lists in a dictionary recursively.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            is_complex = all(isinstance(v, dict) for v in value)
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
    create_tables(
        db_connector=db,
        model=lib.Test,
        markdown_path="./.github/integration/model.md",
    )

    # Check tables
    expected_tables = set(
        [
            "Test",
            "Test_nested",
            "Test_multiple_values",
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
        "Test_id": "!string",
        "name": "string",
        "int_value": "int64",
        "float_value": "float64",
        "bool_value": "boolean",
    }

    assert schema == expected, f"Expected schema '{expected}' but got '{schema}'"

    # Add data
    obj = lib.Test(
        int_value=1,
        float_value=1.0,
        bool_value=True,
        name="Hello",
        multiple_values=[1, 2, 3],
    )

    db.insert(obj)

    expected = {**obj.dict(exclude_unset=True), "Test_id": str(obj.__id__)}
    del expected["multiple_values"]

    entry = db.connection.table("Test").execute().loc[0].to_dict()

    assert entry == expected, f"Expected entry '{expected}' but got '{entry}'"

    # Check if the primitive list table is populated
    assert (
        db.connection.table("Test_multiple_values").count().execute() == 3
    ), "Wrong count for primitive list table"

    # Retrieve the object again
    retrieved = db.get("Test", "name", "Hello")
    to_exclude = {
        "id": True,
        "nested": {
            0: {"id": True},
            1: {"id": True},
        },
    }

    expected = sort_subkeys(obj.dict(exclude=to_exclude))
    retrieved = sort_subkeys(retrieved.dict(exclude=to_exclude))

    assert expected == retrieved, f"Expected object '{obj}' but got '{retrieved}'"
