import pytest

from sdRDM import DataModel
from sdrdm_database import create_tables


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
    create_tables(db_connector=db, model=lib.Test)

    # Check tables
    expected_tables = set(["Test", "nested", "multiple_values"])
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

    expected = {**obj.dict(exclude_unset=True), "id": str(obj.__id__)}
    del expected["multiple_values"]

    entry = db.connection.table("Test").execute().loc[0].to_dict()

    assert entry == expected, f"Expected entry '{expected}' but got '{entry}'"

    # Check if the primitive list table is populated
    assert (
        db.connection.table("multiple_values").count().execute() == 3
    ), "Wrong count for primitive list table"
