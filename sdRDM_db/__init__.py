import ibis

from .dbconnector import DBConnector
from .tablecreator import create_tables

ibis.options.interactive = True  # type: ignore
