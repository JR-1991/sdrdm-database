import ibis
import os
import nest_asyncio

from .dbconnector import DBConnector
from .dbconnector import SupportedBackends
from .tablecreator import create_tables
from .commands import PostgresCommands, MySQLCommands

ibis.options.interactive = True  # type: ignore
nest_asyncio.apply()
