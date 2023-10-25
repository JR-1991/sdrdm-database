# Example: Building and working with an EnzymeML database

The following example demonstrates a practical example of how to build and work with an EnzymeML database. The example is based on the [EnzymeML schema](https://github.com/EnzymeML/enzymeml-specifications.git) and datasest are provided in `datasets`. The example is based on the following steps:

1. Create tables based on the EnzymeML schema
2. Populate the database with data from the EnzymeML datasets
3. Filter the database based on a query for `reactants`
4. Extract the corresponding EnzymeMLDocuments

**Files to look into**

* `populate_db.py` - Sets up the database and adds [datasets](/datasets) found in this example
* `Extract.ipynb` - Demonstrates how to filter a database and recover documentes

## Database setup

This example includes a `docker-compose`recipe to spawn a new instance of a MySQL database. For this, run the following command:

```bash
sudo docker-compose up --build  # Start MySQL database
python populate_db.py           # Populate database
```
