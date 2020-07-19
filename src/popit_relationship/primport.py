import asyncio

import click
from dotenv import load_dotenv

import popit_relationship.db as db
from popit_relationship.common import graph_init, graph_save
from popit_relationship.sync import sync


@click.group()
def reset():
    pass


@reset.command("db")
@click.confirmation_option(
    prompt="The specified database will be deleted, is the data backed up?"
)
def reset_db():
    with db.driver_init() as driver, driver.session() as session:
        session.write_transaction(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))


@reset.command("cache")
@click.confirmation_option(
    prompt="The specified cache will be deleted, is the data backed up?"
)
def reset_cache():
    graph = graph_init()
    graph.clear()
    graph_save(graph)


@click.command("save")
def save():
    with db.driver_init() as driver, driver.session() as session:
        session.write_transaction(db.graph_save, graph_init())


@click.group()
def app():
    pass


app.add_command(reset)
app.add_command(save)
app.add_command(sync)


def main():
    load_dotenv()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(app())


if __name__ == "__main__":
    main()
