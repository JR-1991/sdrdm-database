import collections
from functools import partial
from typing import Any, List, get_origin
from uuid import uuid4


def insert_into_database(
    dataset: "DataModel",
    db: "DBConnector",
    table_name: str = None,
    parent_id: str = None,
    parent_col: str = None,
):
    """Inserts an instance of the sdRDM schema into the database.

    Args:
        dataset (DataModel): An instance of the sdRDM schema.
        db (DBConnector): A connection to the database.
        table_name (str, optional): The name of the table to insert the data into. Defaults to None.
        parent_id (str, optional): The ID of the parent object. Defaults to None.
        parent_col (str, optional): The name of the column containing the parent object ID. Defaults to None.
    """

    if table_name is None:
        table_name = dataset.__class__.__name__

    sub_objects = {}
    to_exclude = set(["id"])
    post_insert = []

    for key, value in dataset:
        field_info = dataset.__fields__[key]
        is_mutliple = get_origin(field_info.outer_type_) is list
        is_obj = hasattr(field_info.type_, "__fields__")

        if is_obj:
            sub_objects[key] = [value] if not is_mutliple else value
            to_exclude.add(key)
        elif is_mutliple and not is_obj:
            kwargs = {
                "table": key,
                "array": value,
                "db": db,
                "parent_col": f"{table_name}_id",
                "parent_id": str(dataset.__id__),
            }

            post_insert.append(partial(_insert_primitive_array, **kwargs))
            to_exclude.add(key)

    to_insert = {
        **dataset.dict(exclude=to_exclude),
        f"{table_name}_id": str(dataset.__id__),
    }

    if parent_id is not None:
        assert parent_col is not None, "Parent column must be specified"
        to_insert[f"{parent_col}_id"] = parent_id

    db.connection.insert(table_name, to_insert)

    for func in post_insert:
        func()

    for sub_table, sub_data in sub_objects.items():
        for sub_dataset in sub_data:
            if _is_empty(sub_dataset):
                continue

            insert_into_database(
                dataset=sub_dataset,
                db=db,
                table_name=sub_table,
                parent_id=str(dataset.__id__),
                parent_col=table_name,
            )


def _is_empty(obj):
    """Checks if the given object is empty.

    Args:
        obj: An object to check for emptiness.

    Returns:
        A boolean indicating whether the object is empty or not.
    """

    return (
        obj.dict(
            exclude_unset=True,
            exclude_none=True,
            exclude={"id"},
        ).values()
        == {}
    )


def _insert_primitive_array(
    table: str,
    array: List[Any],
    db: "DBConnector",
    parent_col: str,
    parent_id: str,
) -> None:
    for value in array:
        to_insert = {
            table: value,
            parent_col: parent_id,
        }
        db.connection.insert(table, to_insert)
