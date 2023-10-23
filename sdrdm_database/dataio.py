from functools import partial
from typing import Any, List, get_origin
from typing import get_origin
import pandas as pd


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

    return (
        obj.dict(
            exclude_unset=True,
            exclude_none=True,
            exclude={"id"},
        ).values()
        == {}
    )


from typing import Any, List


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


def _query_equal(
    table,
    column,
    value,
):
    """
    Returns a boolean expression that can be used to filter a table based on a specific column and value.

    Args:
        table (pandas.DataFrame): The table to filter.
        column (str): The name of the column to filter on.
        value (Any): The value to filter for.

    Returns:
        pandas.Series: A boolean expression that can be used to filter the table.
    """
    return table[column] == value


def _extract_related_rows(
    model,
    table_name,
    id_col,
    attr,
    target,
    query_fun,
    db_connector,
    dataset,
    foreign_key=None,
):
    """Extracts related rows from a database table.

    Args:
        model: The model class.
        table_name: The name of the table to extract rows from.
        id_col: The name of the ID column.
        attr: The attribute to query on.
        target: The target value to query for.
        query_fun: The query function to use.
        db_connector: The database connector.
        dataset: The dataset to update.
        foreign_key: The foreign key to exclude.

    Returns:
        None.
    """
    table = db_connector.connection.table(table_name)

    if query_fun:
        rows = table[query_fun(table, attr, target)].execute()
    else:
        rows = table.execute()

    for _, row in rows.iterrows():
        row = row.where(pd.notnull(row), None)
        to_add = {
            key.replace(f"{table_name}_id", "id"): value
            for key, value in row.items()
            if key != foreign_key
        }

        if isinstance(dataset, list):
            dataset.append(to_add)
        else:
            dataset.update(to_add)

        # Retrieve the ID of the root table
        row_id = row[id_col]

        _process_row(
            model=model,
            id_col=id_col,
            attr=attr,
            row_id=row_id,
            db_connector=db_connector,
            dataset=dataset,
        )


def _process_row(
    model,
    id_col,
    attr,
    row_id,
    db_connector,
    dataset,
):
    """
    Extracts related rows from a sub-table and adds them to the dataset.

    Args:
        model: The model class for the parent table.
        id_col: The name of the ID column for the parent table.
        attr: The attribute being processed.
        row_id: The ID of the row being processed.
        db_connector: The database connector object.
        dataset: The dataset to add the related rows to.

    Returns:
        None
    """
    for attr in model.__fields__.values():
        sub_table_name = f"{model.__name__}_{attr.name}"
        is_obj = hasattr(attr.type_, "__fields__")
        is_multiple = get_origin(attr.outer_type_) is list

        if not is_obj and not is_multiple:
            continue
        elif not is_obj and is_multiple:
            sub_table = db_connector.connection.table(sub_table_name)
            rows = sub_table[sub_table[id_col] == row_id].execute()

            if rows.empty:
                continue

            dataset[attr.name] = rows[attr.name].tolist()
            continue

        if attr.name not in dataset and is_multiple:
            dataset[attr.name] = []
        elif attr.name not in dataset:
            dataset[attr.name] = {}

        # Filter the sub table by the parent ID
        _extract_related_rows(
            model=attr.type_,
            table_name=sub_table_name,
            id_col=f"{model.__name__}_id",
            attr=id_col,
            target=row_id,
            query_fun=_query_equal,
            db_connector=db_connector,
            dataset=dataset[attr.name],
            foreign_key=f"{model.__name__}_id",
        )
