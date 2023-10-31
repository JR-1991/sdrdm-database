from functools import partial
from typing import Any, List, get_origin
from typing import get_origin
from sqlalchemy.orm import Session


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
        sub_key = f"{dataset.__class__.__name__}_{key}"

        if is_obj:
            sub_objects[sub_key] = [value] if not is_mutliple else value
            to_exclude.add(key)
        elif is_mutliple and not is_obj:
            kwargs = {
                "table": sub_key,
                "column": key,
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

    if obj is None:
        return True

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
    column: str,
    array: List[Any],
    db: "DBConnector",
    parent_col: str,
    parent_id: str,
) -> None:
    """
    Inserts a list of primitive values into a database table.

    Args:
        table (str): The name of the table to insert the values into.
        column (str): The name of the column to insert the values into.
        array (List[Any]): The list of primitive values to insert.
        db (DBConnector): The database connector object to use for the insertion.
        parent_col (str): The name of the column that contains the parent ID.
        parent_id (str): The ID of the parent object that the values belong to.
    """
    for value in array:
        to_insert = {
            column: value,
            parent_col: parent_id,
        }
        db.connection.insert(table, to_insert)


def _retrieve_documents(db, table_name):
    """Retrieves all documents from a given table in the database.

    Args:
        db (DBConnector): A DBConnector object representing the database.
        table_name (str): The name of the table to retrieve documents from.

    Returns:
        list: A list of documents retrieved from the table.
    """
    assert (
        db.__class__.__name__ == "DBConnector"
    ), "Database must be a DBConnector object"
    assert table_name in db.__models__, "Table has no associated model"
    assert db.engine, "Database engine must be set"
    assert db.__sqlalchemy_classes__, "SQLAlchemy classes must be set"

    with Session(db.engine) as session:
        model = db.get_table_api(table_name)
        automap_cls = getattr(db.__sqlalchemy_classes__, table_name)
        objects = session.query(automap_cls).all()
        return [model(**_extract_document(obj)) for obj in objects]


def _extract_document(obj):
    """
    Extracts a dictionary representation of an automapped object and its children.

    Args:
        obj: An automapped object.

    Returns:
        A dictionary representation of the object and its children.
    """

    assert _is_automap(obj), "Object must be an automapped object"

    collections = _get_collections(obj)
    multi_names = _extract_attr_names(collections)
    table_name = _get_table_name(obj)
    foreign_keys = _get_foreign_keys(obj)
    forbidden_keys = foreign_keys + collections

    dataset = {
        key.replace(table_name + "_", "", 1): value
        for key, value in obj.__dict__.items()
        if key not in forbidden_keys and not key.startswith("_")
    }

    for collection, multi_name in zip(collections, multi_names):
        collection = getattr(obj, collection)

        if collection == []:
            continue

        dataset[multi_name] = [_extract_document(child) for child in collection]

    return dataset


def _is_automap(obj):
    """
    Checks if an object is an automapped object.

    Args:
        obj: An object.

    Returns:
        True if the object is an automapped object, False otherwise.
    """
    return obj.__module__ == "sqlalchemy.ext.automap"


def _get_collections(obj):
    """
    Gets a list of collections in an automapped object.

    Args:
        obj: An automapped object.

    Returns:
        A list of collections in the automapped object.
    """
    return [attr for attr in obj.__dir__() if attr.endswith("_collection")]


def _extract_attr_names(collections):
    """
    Extracts attribute names from a list of collections.

    Args:
        collections: A list of collections.

    Returns:
        A list of attribute names extracted from the collections.
    """
    return [attr.split("_", 1)[-1].split("_collection", -1)[0] for attr in collections]


def _get_table_name(obj):
    """
    Gets the name of the table associated with an automapped object.

    Args:
        obj: An automapped object.

    Returns:
        The name of the table associated with the automapped object.
    """
    return obj.__class__.__table__.name


def _get_foreign_keys(obj):
    """
    Gets a list of foreign keys associated with an automapped object.

    Args:
        obj: An automapped object.

    Returns:
        A list of foreign keys associated with the automapped object.
    """
    return [key.column.name for key in obj.__class__.__table__.foreign_keys]
