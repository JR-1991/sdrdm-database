import os
import tempfile
from typing import Dict
from io import StringIO

from sdRDM import DataModel
from sdRDM.generator.codegen import generate_api_from_parser
from sdRDM.markdown.markdownparser import MarkdownParser
from sdRDM.tools.gitutils import _import_library


def convert_md_to_json(
    md_content: str,
):
    """Converts a markdown file to a JSON object.

    Args:
        path (str): The path to the markdown file.

    Returns:
        dict: A JSON string representing the markdown file.
    """
    return MarkdownParser.parse(StringIO(md_content)).json(indent=2)


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

        return DataModel._extract_modules(
            _import_library(api_loc, libname),
            links={},
        )
