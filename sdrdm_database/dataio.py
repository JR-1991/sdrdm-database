from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple, get_origin
from typing import get_origin
from joblib import Parallel, delayed
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


def _extract_related_rows(
    table,
    id_col: str,
    db: "DBConnector",
    model: "DataModel",
    query_fun: Optional[Callable] = None,
    n_jobs: int = -1,
    MAX_ROWS: int = 20,
) -> List[Tuple[int, Dict[str, Any]]]:
    """Extracts related rows from a database table.

    Args:
        table: The database table to extract rows from.
        id_col: The name of the column containing the row IDs.
        db: The database connection object.
        model: The Pydantic model representing the table schema.
        query_fun: Optional function to filter rows before extraction.
        n_jobs: Number of parallel jobs to use for extraction.
        MAX_ROWS: Maximum number of rows to extract.

    Returns:
        A list of tuples, where each tuple contains the row ID and a dictionary
        of the row's attribute values.
    """

    # Extract data
    if query_fun:
        rows = table[query_fun(table)].execute()
    else:
        rows = table.execute().iloc[0:MAX_ROWS]

    subset = list(model.__fields__.keys())
    subset.remove("id")

    # Find out, which attributes are objects
    obj_subset = [
        (
            attr.type_,
            attr.name,
            get_origin(attr.outer_type_) == list,
        )
        for attr in model.__fields__.values()
        if hasattr(attr.type_, "__fields__") or get_origin(attr.outer_type_) == list
    ]

    if db.dbtype.value == "mysql":
        data = []
        for _, row in rows.iterrows():
            data.append(
                _process_row(
                    row=row,
                    subset=subset,
                    obj_subset=obj_subset,
                    id_col=id_col,
                    model=model,
                    db=db,
                )
            )

        return data

    return Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(_process_row)(
            row=row,
            subset=subset,
            obj_subset=obj_subset,
            id_col=id_col,
            model=model,
            db=db,
        )
        for _, row in rows.iterrows()
    )


def _process_row(
    row,
    subset,
    obj_subset,
    id_col,
    model,
    db,
):
    """
    Processes a single row of data from a database table.

    Args:
        row (pandas.Series): A single row of data from a database table.
        subset (List[str]): A list of column names to include in the processed data.
        obj_subset (List[Tuple[type, str, bool]]): A list of tuples representing related objects to include in the processed data. Each tuple contains:
            - A reference to the related object's model class
            - The name of the related object's attribute in the processed data
            - A boolean indicating whether the related object is a single object or a list of objects
        id_col (str): The name of the column containing the primary key ID for the table.
        model (type): The model class for the table being processed.
        db (Database): The database object containing the table being processed.

    Returns:
        dict: A dictionary containing the processed data for the input row, including related objects as specified in obj_subset.
    """

    subset = [col for col in subset if col in row]
    row = row.where(pd.notnull(row), None)
    dataset = row[subset].to_dict()
    dataset["id"] = row[id_col]

    for (
        sub_model,
        name,
        is_multi,
    ) in obj_subset:
        sub_table_name = f"{model.__name__}_{name}"
        sub_table = db.connection.table(sub_table_name)
        is_obj = hasattr(sub_model, "__fields__")

        if is_multi and not is_obj:
            filtered = sub_table[sub_table[f"{model.__name__}_id"] == row[id_col]]
            dataset[name] = filtered[name].execute().values.tolist()
            continue

        res = _extract_related_rows(
            table=sub_table,
            id_col=f"{sub_table_name}_id",
            query_fun=lambda table: table[id_col] == row[id_col],
            db=db,
            model=sub_model,
            n_jobs=1,
        )

        if not res:
            continue

        if is_multi:
            dataset[name] = res
        else:
            dataset[name] = res[0]

    return dataset
