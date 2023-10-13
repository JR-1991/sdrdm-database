import ibis
import os

from .dbconnector import DBConnector
from .dbconnector import SupportedBackends
from .tablecreator import create_tables
from .commands import PostgresCommands, MySQLCommands

ibis.options.interactive = True  # type: ignore
