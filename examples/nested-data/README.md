# Dealing with nested data models

The following example demonstrates how to set up a MySQL database using Docker and sdRDM-DB. Here we will make use of the `create_tables` functionality to create a database with two tables (see [markdown model](model.md)) that are related to each other. Ultimately, we will store a nested data structure in the database.

## Database setup

This example includes a `docker-compose`recipe to spawn a new instance of a MySQL database. For this, run the following command:

```bash
sudo docker-compose up --build
```
