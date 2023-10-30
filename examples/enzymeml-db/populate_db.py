"""
This script populates the database with EnzymeML documents.
The documents are generated from the EnzymeML specification.
"""

import toml
import glob

from sdRDM import DataModel
from sdrdm_database import DBConnector

# Connect to the database
db = DBConnector(**toml.load(open("./env.toml")))

# Create tables from the EnzymeML schema
db.create_tables(
  "EnzymeMLDocument",
  "https://github.com/EnzymeML/enzymeml-specifications.git"
)

# Add some datasets
datasets = [DataModel.parse(file)[0] for file in glob.glob("./datasets/*.json")]
db.insert(*datasets, verbose=True)
