import os
import asyncio
from dotenv import load_dotenv, set_key
import typer
import time

from indexer.users import main as user_indexer_main
from indexer.casts import main as cast_indexer_main
from indexer.eth import main as eth_indexer_main
from packager.package import main as packager_main
from packager.download import main as downloader_main
from query import (
    execute_raw_sql,
    execute_natural_language_query,
    execute_advanced_query
)

load_dotenv()
warpcast_hub_key = os.getenv('WARPCAST_HUB_KEY')
openai_api_key = os.getenv('OPENAI_API_KEY')

app = typer.Typer()

indexer_app = typer.Typer()
app.add_typer(indexer_app, name="indexer")


def update_env_variables():
    dotenv_path = '.env'
    global warpcast_hub_key, openai_api_key
    new_warpcast_hub_key = typer.prompt("Enter your Warpcast Hub key")
    new_openai_api_key = typer.prompt("Enter your OpenAI API key")
    set_key(dotenv_path, "WARPCAST_HUB_KEY", new_warpcast_hub_key)
    set_key(dotenv_path, "OPENAI_API_KEY", new_openai_api_key)
    warpcast_hub_key = new_warpcast_hub_key
    openai_api_key = new_openai_api_key


@app.command()
def init():
    """Initialize the environment with OpenAI and Warpcast Hub keys."""
    global warpcast_hub_key, openai_api_key

    if warpcast_hub_key and openai_api_key:
        typer.echo("Environment variables detected:")
        typer.echo(f"WARPCAST_HUB_KEY: {warpcast_hub_key}")
        typer.echo(f"OPENAI_API_KEY: {openai_api_key}")
        if typer.confirm("Do you want to overwrite the existing values?"):
            update_env_variables()
    else:
        update_env_variables()


@indexer_app.command("all")
def refresh_all_data():
    """Refresh all data in the DB."""
    asyncio.run(user_indexer_main())
    cast_indexer_main()
    asyncio.run(eth_indexer_main())


@indexer_app.command("user")
def refresh_user_data():
    """Refresh user data."""
    asyncio.run(user_indexer_main())


@indexer_app.command("cast")
def refresh_cast_data():
    """Refresh cast data."""
    cast_indexer_main()


@indexer_app.command("eth")
def refresh_eth_data():
    """Refresh onchain Ethereum data."""
    asyncio.run(eth_indexer_main())


@app.command()
def download():
    """Download datasets."""
    downloader_main()
    print()

    if typer.confirm("(Optional) do you want to initialize the environment with OpenAI and Warpcast Hub keys?"):
        init()


@app.command()
def package():
    """Package and zip datasets."""
    packager_main()


# # TODO: --csv flag
# @app.command()
# def query(query: str = typer.Argument(None, help='Query Farcaster data with natural language.'),
#           raw: str = typer.Option(None, help='Query Farcaster data with SQL.'),
#           advanced: str = typer.Option(None, help='Query Farcaster data with Langchain\'s SQL agent.')):

#     if raw:
#         execute_raw_sql(raw)
#     elif query:
#         execute_natural_language_query(query)
#     elif advanced:
#         execute_advanced_query(advanced)
#     else:
#         typer.echo("Please provide either --raw, --query, or --advanced option.")


@app.command()
def query(query: str = typer.Argument(None, help='Query Farcaster data with natural language.'),
          raw: str = typer.Option(None, help='Query Farcaster data with SQL.'),
          advanced: str = typer.Option(None, help='For testing purposes.'),
          csv: bool = typer.Option(False, help='Save the result to a CSV file. Format: {unix_timestamp}.csv')):

    if raw:
        df = execute_raw_sql(raw)
        print(df)
    elif query:
        df = execute_natural_language_query(query)
        print(df)
    elif advanced:
        execute_advanced_query(advanced)
    else:
        typer.echo("Please provide either --raw, --query, or --advanced option.")
        return

    if csv is True and df is not None:
        df.write_csv(f"{int(time.time())}.csv")


if __name__ == "__main__":
    app()
