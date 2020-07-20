import asyncio
from collections import defaultdict
from functools import reduce

import click
import matplotlib.pyplot as plt
import networkx as nx
from dotenv import load_dotenv
from toolz.dicttoolz import merge

import popit_relationship.db as db
from popit_relationship.common import graph_init, graph_save
from popit_relationship.db import arrow_get_type
from popit_relationship.sync import node_is_class, sync


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


@click.command("visualize")
@click.argument("node_list", nargs=-1)
@click.option("--depth", type=click.INT, default=3)
def visualize(node_list, depth):
    graph = graph_init()

    neighbourhood = node_populate_neighbours(graph, set(node_list), depth)

    subgraph = nx.subgraph(graph, neighbourhood)
    layout = nx.spring_layout(subgraph)
    nx.draw_networkx_nodes(
        subgraph, layout, with_labels=True,
    )
    nx.draw_networkx_labels(
        subgraph,
        layout,
        labels={
            node: data.get("name", data.get("label", node))
            for node, data in subgraph.nodes(True)
        },
    )
    nx.draw_networkx_edges(
        subgraph, layout,
    )
    nx.draw_networkx_edge_labels(
        subgraph,
        layout,
        edge_labels=reduce(
            lambda current, incoming: merge(
                current,
                {
                    (incoming[0], incoming[1]): arrow_get_type(
                        subgraph.get_edge_data(*incoming).get("uri", incoming[-1])
                    )
                },
            ),
            subgraph.edges,
            {},
        ),
    )
    plt.show()


def node_populate_neighbours(graph, node_list, depth_max, depth_current=0):
    to_return = set()

    if depth_max == depth_current:
        to_return = node_list
    else:
        result = reduce(
            lambda current, incoming: current.union(
                set()
                if node_is_class(incoming)
                else set(
                    node
                    for node in nx.all_neighbors(graph, incoming)
                    if not node_is_class(node)
                ).union(set([incoming]))
            ),
            node_list,
            set(),
        )

        if len(result) == len(node_list):
            to_return = node_list
        else:
            to_return = node_populate_neighbours(
                graph, result, depth_max, depth_current + 1
            )

    return to_return


@click.command("save")
def save():
    with db.driver_init() as driver, driver.session() as session:
        session.write_transaction(db.graph_save, graph_init())


@click.group()
def app():
    pass


app.add_command(reset)
app.add_command(visualize)
app.add_command(save)
app.add_command(sync)


def main():
    load_dotenv()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(app())


if __name__ == "__main__":
    main()
