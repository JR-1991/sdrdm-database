import glob
import os
import tempfile
from io import StringIO
from typing import Dict, Tuple, get_args
import git

from pydantic.fields import FieldInfo
from sdRDM.generator.utils import extract_modules
from sdRDM.generator.codegen import generate_api_from_parser
from sdRDM.markdown.markdownparser import MarkdownParser
from sdRDM.tools.gitutils import _import_library
import validators


def convert_md_to_json(
    md_content: str,
):
    """Converts a markdown file to a JSON object.

    Args:
        path (str): The path to the markdown file.

    Returns:
        dict: A JSON string representing the markdown file.
    """
    return MarkdownParser.parse(StringIO(md_content)).model_dump_json(indent=2)


def rebuild_api(
    specifications: Dict,
    libname: str,
):
    """
    Rebuilds the API from the given JSON string and library name.

    Args:
        specifications (dict): The API specifications.
        libname (str): The name of the library.

    Returns:
        The extracted modules from the rebuilt API.
    """
    parser = MarkdownParser.parse_obj(specifications)

    with tempfile.TemporaryDirectory() as tmpdir:
        generate_api_from_parser(
            parser=parser,
            dirpath=tmpdir,
            libname=libname,
        )

        api_loc = os.path.join(tmpdir, libname)

        return extract_modules(
            _import_library(api_loc, libname),
            links={},
        )


def extract_lib_relations(lib):
    """
    Extracts the relations of a given library.

    Args:
        lib: The library object for which relations need to be extracted.

    Returns:
        A dictionary containing the extracted relations.
    """
    relations = {}
    for obj in lib.__dict__.values():
        if not hasattr(obj, "model_fields"):
            continue

        relations.update(get_relations(obj))

    return relations


def get_relations(obj):
    """
    Returns a dictionary of relations for the given object.

    Parameters:
        obj (object): The object for which relations need to be extracted.

    Returns:
        dict: A dictionary containing the relations of the object.
    """
    relations = {}
    for name, field in obj.model_fields.items():
        args = get_args(field.annotation)
        has_object = any(hasattr(arg, "model_fields") for arg in args)

        if not has_object:
            continue

        dtype = _get_annotation_object(field.annotation)
        relation_name = f"{obj.__name__}_{name}_{dtype.__name__}"
        relations[relation_name] = _get_attribute_relation(field, obj.__name__)

    return relations


def _get_attribute_relation(
    field: FieldInfo,
    obj_name: str,
) -> Tuple[Dict, Dict]:
    """
    Returns a dictionary representing the attribute relation for a given field and object name.

    Args:
        field (FieldInfo): The field information.
        obj_name (str): The name of the object.

    Returns:
        Tuple[Dict, Dict]: A tuple containing two dictionaries representing the attribute relations.
        The first dictionary represents the attribute relation for the object name, with the following keys:
            - "column": The column name representing the object ID.
            - "references": A set containing the object name.
        The second dictionary represents the attribute relation for the field type, with the following keys:
            - "column": The column name representing the field type ID.
            - "references": A set containing the field type name.
    """

    target = _get_annotation_object(field.annotation)

    return (
        {
            "column": f"{obj_name}_id",
            "references": obj_name,
        },
        {
            "column": f"{target.__name__}_id",
            "references": target.__name__,
        },
    )


def _get_annotation_object(annotation):
    """
    Returns the object from the given annotation.

    Args:
        annotation: The annotation object.

    Returns:
        The object from the annotation.

    Raises:
        ValueError: If no object is found in the annotation.
    """
    args = get_args(annotation)

    if len(args) == 1:
        return args[0]
    elif len(args) > 1:
        return next(
            filter(
                lambda arg: hasattr(arg, "model_fields"),
                args,
            )
        )
    else:
        raise ValueError("No object found in annotation")


def get_md_content(markdown_path: str) -> str:
    """
    Fetches the content of a markdown file.

    Args:
        markdown_path (str): The path to the markdown file.

    Returns:
        str: The content of the markdown file.
    """
    if validators.url(markdown_path):
        print("├── Fetching markdown model from GitHub")
        with tempfile.TemporaryDirectory() as tmpdirname:
            return _fetch_specs(markdown_path, tmpdirname)
    else:
        return open(markdown_path).read()


def _fetch_specs(url: str, tmpdirname: str):
    """
    Fetches the specifications from the given URL and temporary directory.

    Args:
        url (str): The URL of the repository to clone.
        tmpdirname (str): The path of the temporary directory to clone the repository into.

    Returns:
        str: The content of the fetched markdown file.

    Raises:
        ValueError: If no markdown files are found in the specified directory.
        ValueError: If more than one markdown file is found in the specified directory.
    """
    git.Repo.clone_from(url, tmpdirname)

    schema_loc = os.path.join(tmpdirname, "specifications")
    md_files = glob.glob(f"{schema_loc}/*.md")

    if len(md_files) == 0:
        raise ValueError(f"No markdown files found in {schema_loc}")
    elif len(md_files) > 1:
        raise ValueError(f"More than one markdown file found in {schema_loc}")

    return open(md_files[0]).read()
