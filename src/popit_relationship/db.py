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
