from copy import deepcopy
from typing import Dict

from sqlalchemy import inspect
from bigtree import (
    Node,
    dict_to_tree,
    list_to_tree_by_relation,
    prune_tree,
    tree_to_dict,
)


def get_model_tree(
    db: "DBConnector",
    model_name: str,
):
    """
    Given a database connector and a model name, returns a tree structure of the model's table.

    Args:
        db (DBConnector): A DBConnector object representing the database connection.
        model_name (str): The name of the model whose table is to be returned.

    Returns:
        A tree structure of the model's table.
    """
    assert (
        db.__class__.__name__ == "DBConnector"
    ), "Database must be a DBConnector object"

    assert (
        model_name in db.__sqlalchemy_classes__
    ), f"Table '{model_name}' has no associated model"

    parents = []
    relations = set()

    _construct_table_tree(
        db=db,
        model_name=model_name,
        parents=parents,
        relations=relations,
    )

    return list_to_tree_by_relation(list(relations))


def _construct_table_tree(
    db,
    model_name,
    parents,
    relations,
):
    """Constructs a tree of related tables for a given model.

    Args:
        db: The database object.
        model_name: The name of the model to construct the tree for.
        parents: A list of parent models.
        relations: A set of related models.

    Returns:
        A set of related models.
    """
    parents.append(model_name)
    relationships = _get_relationships(db, model_name, parents)
    relations.update(relationships)

    for _, child in relationships:
        relations.update(_get_relationships(db, child, parents))
        _construct_table_tree(db, child, parents, relations)

    return relations


def _get_relationships(db, model_name, parents):
    """
    Given a database object, a model name, and a list of parent model names,
    returns a list of tuples representing the relationships between the model
    and its child models.

    Args:
        db: A SQLAlchemy database object.
        model_name: A string representing the name of the model to retrieve
            relationships for.
        parents: A list of strings representing the names of parent models to
            exclude from the returned relationships.

    Returns:
        A list of tuples, where each tuple contains two strings representing
        the names of the related models.
    """

    model = getattr(db.__sqlalchemy_classes__, model_name)
    return [
        (model_name, rel.target.name)
        for rel in inspect(model).relationships.values()
        if not rel.target.name in parents
    ]


def _prune_by_criteria(
    tree: Node,
    criteria: Dict[str, Dict[str, str]],
) -> Node:
    """
    Prunes a tree of tables to only include those relevant to a given set of criteria.

    Args:
        tree: A `bigtree.Node` instance representing the tree of tables to prune.
        criteria: A dictionary representing the criteria to filter the query by.

    Returns:
        A `bigtree.Node` instance representing the pruned tree of tables.
    """
    tree = deepcopy(tree)
    available_paths = list(tree_to_dict(tree).keys())
    to_query = list(criteria.keys())
    prunable = set()
    trees = {}

    for table in to_query:
        for path in available_paths:
            if path.endswith(table):
                prunable.add(path)

    for to_prune in prunable:
        pruned = tree_to_dict(prune_tree(tree, to_prune))
        trees.update(pruned)

    return dict_to_tree(trees)
