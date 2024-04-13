from typing import Any, Dict, Tuple, get_args, get_origin
from sdRDM import DataModel

import asyncio

async def map_multiple_to_sqlalchemy(objs, db):
    """
    Maps a list of Python objects to a list of SQLAlchemy model objects.

    Args:
        objs: The list of DataModel objects to be mapped.
        db: The SQLAlchemy database object.

    Returns:
        A list of mapped SQLAlchemy model objects.
    """

    tasks = [_map_to_sqlalchemy(obj, db) for obj in objs]
    return await asyncio.gather(*tasks)

async def _map_to_sqlalchemy(obj, db):
    """
    Maps a Python object to a SQLAlchemy model object.

    Args:
        obj: The Python object to be mapped.
        db: The SQLAlchemy database object.

    Returns:
        The mapped SQLAlchemy model object.
    """

    sdrdm_model = db._models[obj.__class__.__name__]
    sql_model = db._sqlalchemy_classes[obj.__class__.__name__]

    data = {}
    tasks = [
        _process_attribute(
            tup=tup,
            obj=obj,
            sdrdm_model=sdrdm_model,
            db=db,
            data=data,
        )
        for tup in obj
    ]

    await asyncio.gather(*tasks)

    return sql_model(**data)


async def _process_attribute(
    tup: Tuple[str, Any],
    obj: DataModel,
    sdrdm_model: DataModel,
    db: "DBConnector",
    data: Dict,
):
    """
    Process an attribute of a DataModel object and update the data dictionary.

    Args:
        tup (Tuple[str, Any]): A tuple containing the attribute name and its value.
        obj (DataModel): The DataModel object being processed.
        sdrdm_model (DataModel): The DataModel class definition.
        db (DBConnector): The database connector.
        data (Dict): The dictionary to store the processed data.

    Returns:
        None
    """

    attr, value = tup

    if value is None:
        return

    if attr == "id":
        data[attr] = str(obj._id)
        return

    dtype = sdrdm_model.model_fields[attr].annotation
    is_list = get_origin(dtype) == list
    complexes = [d for d in get_args(dtype) if issubclass(d, DataModel)]

    if is_list and complexes:
        tasks = [_map_to_sqlalchemy(v, db) for v in value]
        result = await asyncio.gather(*tasks)

        data[attr] = result
    elif not is_list and complexes:
        data[attr] = await _map_to_sqlalchemy(value, db)
    elif is_list:
        data[attr] = value
    else:
        data[attr] = value
