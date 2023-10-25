# sdrdm-database

![Tests](https://github.com/JR-1991/sdrdm-database/actions/workflows/test.yml/badge.svg) ![Integration](https://github.com/JR-1991/sdrdm-database/actions/workflows/integration.yml/badge.svg)

This is the sdRDM DB interface to create tables from a markdown model and insert/retrieve data from it.

## Installation

To get started with the sdRDM DB interface, you can install it via pip:

```bash
# Base installation
pip install sdrdm_database

# MySQL
pip install "sdrdm_database[mysql]"

# PostgreSQL
pip install "sdrdm_database[postgres]"
```

or directly from the GitHub repository:

```bash
pip install git+https://github.com/JR-1991/sdrdm-database.git
```

## Examples

* [Setting up a MySQL database and add an EnzymeML model](./examples/mysql/)
* [Setting up a PostgreSQL database and add an EnzymeML model](./examples/postgres/)
* [Dealing with nested data models](./examples/nested-data/)
* [Query a MySQL database using GraphQL](./examples/graphql/)
* [Application: Building and working with an EnzymeML database](./examples/enzymeml-db/)
