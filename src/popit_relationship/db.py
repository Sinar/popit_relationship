import os

from neo4j import GraphDatabase


def driver_init():
    return GraphDatabase.driver(
        os.environ.get("NEO4J_URI", "bolt:localhost:7687"),
        auth=tuple(os.environ.get("NEO4J_AUTH", "neo4j/abc123").split("/")),
    )
