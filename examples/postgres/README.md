# Setting up a PostgreSQL database and add an EnzymeML model

The following example demonstrates how to set up a PostgreSQL database using Docker and sdRDM-DB. Here we will make use of the `create_tables` functionality to enrich the database by parts of the EnzymeML model to store `proteins` and `reactants`.

## Database setup

This example includes a `docker-compose`recipe to spawn a new instance of a PostgreSQL database. For this, run the following command:

```bash
mkdir data
sudo docker-compose up --build
```
