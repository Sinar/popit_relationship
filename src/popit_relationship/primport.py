import asyncio
from urllib.parse import urlsplit

import click
from dotenv import load_dotenv

from popit_relationship.common import graph_init, graph_save
from popit_relationship.db import driver_init
from popit_relationship.sync import sync

KEY_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


@click.group()
def reset():
    pass


@reset.command("db")
@click.confirmation_option(
    prompt="The specified database will be deleted, is the data backed up?"
)
def reset_db():
    with driver_init() as driver, driver.session() as session:
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
    with driver_init() as driver, driver.session() as session:
        graph = graph_init()
        session.write_transaction(save_graph, graph)


def save_graph(tx, graph):
    for node, dest, key in graph.edges:
        node_save(tx, graph, node)

        if not key == KEY_TYPE:
            node_save(tx, graph, dest)

            tx.run(
                f"""
                MATCH (source {{id: $source_id}})
                MATCH (dest {{id: $dest_id}})
                MERGE (source)-[rel:{urlsplit(key).fragment} {{predicate: $key}}]->(dest)
                """,
                source_id=node,
                dest_id=dest,
                key=key,
            )


def node_get_type(graph, node):
    try:
        return [j for i, j, k in graph.edges if i == node and k == KEY_TYPE][0]
    except IndexError:
        return False


def node_save(tx, graph, node):
    if node_get_type(graph, node):
        tx.run(
            f"MERGE (node:{urlsplit(node_get_type(graph, node)).fragment} {{id: $id}})",
            id=node,
        )

        tx.run(
            f"""
            MATCH (node:{urlsplit(node_get_type(graph, node)).fragment} {{id: $id}})
            SET node = $attributes
            """,
            id=node,
            attributes=dict(graph.nodes[node]),
        )
    else:
        tx.run(
            """
            MERGE (node {id: $id})
            """,
            id=node,
        )


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
