import ibis

from ibis.expr.types.relations import Table
from typing import Optional, Union, get_origin


def join_related(
    db: "DBConnector",
    table: Union[str, Table],
):
    """
    Joins a table with its corresponding model table using the table's ID column.

    Args:
        db: A DBConnector instance.
        table: A string or ibis.Table representing the table to join.

    Returns:
        A joined table containing the related rows from the model table.
    """

    assert isinstance(table, str) or isinstance(
        table, Table
    ), "Table must be a string or ibis.Table"

    if isinstance(table, str):
        table = db.connection.table(table)

    table_name = table.get_name()
    model = db.get_table_api(table_name)

    return _join_related_rows(
        table=table,
        model=model,
        predicate=f"{table_name}_id",
        path=model.__name__,
        db=db,
    )


def _join_related_rows(
    table: Table,
    model: "DataModel",
    predicate: str,
    db: "DBConnector",
    path: str,
    joined: Optional[ibis.Expr] = None,
):
    """Recursively joins related rows in a table.

    Args:
        table (ibis.Table): The table to join.
        model (Type): The model type.
        predicate (str): The predicate to use for the join.
        path (str): The path to the table.
        db (Database): The database.
        joined (Optional[ibis.Expr]): The joined expression.

    Returns:
        ibis.Expr: The joined expression.
    """

    for attr in model.__fields__.values():
        is_obj = hasattr(attr.type_, "__fields__")
        is_multiple = get_origin(attr.outer_type_) is list
        rname = path + "_" + attr.name + "_" + "{name}"

        if not is_obj and not is_multiple:
            continue

        to_join = db.connection.table(f"{model.__name__}_{attr.name}")

        if to_join.count().execute() == 0:
            continue

        if joined is None:
            joined = ibis.join(
                table,
                to_join,
                predicates=predicate,
                rname=rname,
            )
        else:
            joined = joined.join(
                to_join,
                predicates=predicate,
                rname=rname,
            )

        joined = _join_related_rows(
            table=to_join,
            model=attr.type_,
            joined=joined,
            predicate=f"{model.__name__}_{attr.name}_id",
            db=db,
            path=model.__name__ + "_" + attr.name,
        )

    return joined


def query_equal(
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
