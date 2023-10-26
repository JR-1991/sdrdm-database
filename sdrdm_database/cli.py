import toml
import typer

from sdrdm_database import DBConnector

app = typer.Typer(no_args_is_help=True)


@app.command()
def create(
    root_obj: str = typer.Option(
        ...,
        "-o",
        "--root-obj",
        help="The root object to create the DB from",
    ),
    model_path: str = typer.Option(
        ...,
        "-p",
        "--model-path",
        help="Path or URL to the data model",
    ),
    env_file: str = typer.Option(
        ...,
        "-e",
        "--env-file",
        help="Path to the .env file for the DB connection",
    ),
):
    """
    Creates a new database using the specified root object and data model.

    Args:
        root_obj (str): The root object to create the DB from.
        model_path (str): Path or URL to the data model.
        env_file (str): Path to the .env file for the DB connection.
    """
    db = DBConnector(**toml.load(open(env_file)))
    db.create_tables(root_obj, model_path)


if __name__ == "__main__":
    app()
