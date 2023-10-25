import os

from sdrdm_database import DBConnector
from sdrdm_database.commands import PostgresCommands, MySQLCommands


def test_commands():
    # Set global testing to NOT connect
    os.environ["TESTING_STAGE"] = "unit_tests"

    # MySQL case
    db = DBConnector(
        db_name="Test",
        username="root",
        password="root",
        host="localhost",
        port=3306,
        dbtype="mysql",
    )

    assert db.__commands__ == MySQLCommands, "Wrong commands class"

    # Postgres case
    db = DBConnector(
        db_name="Test",
        username="root",
        password="root",
        host="localhost",
        port=5432,
        dbtype="postgres",
    )

    assert db.__commands__ == PostgresCommands, "Wrong commands class"
