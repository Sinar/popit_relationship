import os

from neo4j import GraphDatabase


def driver():
    return GraphDatabase.driver(
        os.environ.get("NEO4J_URI", "bolt:localhost:7687"),
        auth=tuple(os.environ.get("NEO4J_AUTH", "neo4j/abc123").split("/")),
    )


def init(session):
    # do the actual delete here
    # https://stackoverflow.com/questions/23310114/how-to-reset-clear-delete-neo4j-database
    pass
