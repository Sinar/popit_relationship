import asyncio
import os
import pickle
from functools import wraps

import networkx as nx
from networkx.exception import NetworkXError

CACHE_PATH_DEFAULT = "./primport-cache.gpickle"
GRAPHML_PATH_DEFAULT = "./primport-cache.graphml"
KEY_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
KEY_RELATIONSHIP = "http://purl.org/vocab/relationship/Relationship"


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def graph_init():
    try:
        with open(os.environ.get("CACHE_PATH", CACHE_PATH_DEFAULT), "rb") as cache_file:
            return pickle.load(cache_file)
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
    with open(os.environ.get("CACHE_PATH", CACHE_PATH_DEFAULT), "wb") as cache_file:
        return pickle.dump(graph, cache_file)


def graph_export_graphml(graph, path=None):
    return nx.write_graphml(
        graph, path or os.environ.get("GRAPHML_PATH", GRAPHML_PATH_DEFAULT)
    )


def node_get_type(graph, node):
    try:
        return [j for i, j, k in graph.edges if i == node and k == KEY_TYPE][0]
    except IndexError:
        return False
