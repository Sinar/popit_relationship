import os
from urllib.parse import urlsplit

from neo4j import GraphDatabase

from popit_relationship.common import KEY_TYPE, node_get_type


def driver_init():
    return GraphDatabase.driver(
        os.environ.get("NEO4J_URI", "bolt:localhost:7687"),
        auth=tuple(os.environ.get("NEO4J_AUTH", "neo4j/abc123").split("/")),
    )


def graph_save(tx, graph):
    for node, dest, key in graph.edges:
        node_save(tx, graph, node)

        if not key == KEY_TYPE:
            node_save(tx, graph, dest)

            node_save_relationship(tx, graph, node, dest, key)


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


def node_save_relationship(tx, graph, node, dest, key):
    data = graph.get_edge_data(node, dest, key)
    if data:
        predicate = data.get("uri", key)

        tx.run(
            f"""
            MATCH (source {{id: $source_id}})
            MATCH (dest {{id: $dest_id}})
            MERGE (source)
                -[rel:{arrow_get_type(predicate)}
                    {{predicate: $predicate, key: $key, {(', ').join('data_{key}: $data_{key}'.format(key=data_key) for data_key, value in data.items() if value)}}}]->
                (dest)
            """,
            source_id=node,
            dest_id=dest,
            predicate=predicate,
            key=key,
            **{f"data_{key}": value for key, value in data.items() if value},
        )
    else:
        tx.run(
            f"""
            MATCH (source {{id: $source_id}})
            MATCH (dest {{id: $dest_id}})
            MERGE (source)-[rel:{arrow_get_type(key)} {{predicate: $key, key: $key}}]->(dest)
            """,
            source_id=node,
            dest_id=dest,
            key=key,
        )


def arrow_get_type(uri):
    return urlsplit(uri).fragment or urlsplit(uri).path.split("/")[-1]
