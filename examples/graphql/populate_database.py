import toml

from sdRDM import DataModel
from sdrdm_database import DBConnector

# Establish a connection to the database
config = toml.load(open("./env.toml"))
del config["table"]

db = DBConnector(**config)

# Fetch the sdRDM schema
lib = DataModel.from_markdown("./model.md")

# Create tables for the sdRDM schema
db.create_tables(
    model=lib.Root,
    markdown_path="./model.md",
)

# Create datasets using the API that has been directly generated from the DB
model = db.get_table_api("Root")

dataset1 = model(value=100.0, some_values=["Hello", "World"])
dataset1.add_to_nested(another_value="something", value=220.0)
dataset1.add_to_nested(another_value="something else", value=220.0)

# And another one
dataset2 = model(value=20.0, some_values=["Whats", "up"])
dataset2.add_to_nested(another_value="hi there", value=220.0)
dataset2.add_to_nested(another_value="this is dataset 2", value=220.0)

# Insert the new dataset into the database
db.insert(dataset1, dataset2, verbose=True)
