import click
from dotenv import load_dotenv
from neo4j import GraphDatabase
from functools import wraps
import os
import asyncio
import aiohttp
import ujson


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def db_driver():
    return GraphDatabase.driver(
        os.environ.get("NEO4J_URI", "bolt:localhost:7687"),
        auth=tuple(os.environ.get("NEO4J_AUTH", "neo4j/abc123").split("/")),
    )


def db_init(session):
    # do the actual delete here
    # https://stackoverflow.com/questions/23310114/how-to-reset-clear-delete-neo4j-database
    pass


@click.group()
@coro
async def app():
    pass


@click.group()
def sync():
    pass


@sync.command("person")
@click.confirmation_option(
    prompt="The specified database will be deleted, is the data backed up?"
)
@coro
async def sync_person():
    print("sync person")

    async with aiohttp.ClientSession(
        headers={"Accept": "application/json"}, json_serialize=ujson.dumps
    ) as session_api:
        with db_driver() as driver, driver.session() as session_db:
            db_init(session_db)

            async with session_api.get(
                os.environ.get(
                    "API_ENDPOINT", "https://politikus.sinarproject.org/@search"
                ),
                params={"portal_type": "Person"},
            ) as response:
                print(await response.json())


@sync.command("relationship")
def sync_relationship():
    print("sync relationship")


def main():
    load_dotenv()

    app.add_command(sync)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(app())


if __name__ == "__main__":
    main()
