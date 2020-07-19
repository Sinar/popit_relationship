import asyncio
import os
from functools import wraps

import networkx as nx
from networkx.exception import NetworkXError

CACHE_PATH_DEFAULT = "./primport-cache.gpickle"
KEY_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def graph_init():
    try:
        return nx.read_gpickle(os.environ.get("CACHE_PATH", CACHE_PATH_DEFAULT))
    except FileNotFoundError:
        graph = nx.MultiDiGraph()
        graph_save(graph)

        return graph


def graph_prune(graph, node_type):
    try:
        for source in graph.predecessors(node_type):
            for dest in graph.successors(source):
                for key, _ in graph.succ[source][dest].items():
                    graph.remove_edges_from((source, dest, key))

            if graph.in_degree(source) == 0:
                graph.remove_node(source)

    except NetworkXError:
        pass


def graph_save(graph):
    return nx.write_gpickle(graph, os.environ.get("CACHE_PATH", CACHE_PATH_DEFAULT))


def node_get_type(graph, node):
    try:
        return [j for i, j, k in graph.edges if i == node and k == KEY_TYPE][0]
    except IndexError:
        return False
