import os
import time
from functools import reduce
from urllib.parse import parse_qs, urlsplit

import aiohttp
import click
import ujson
from toolz.dicttoolz import get_in, valfilter

from popit_relationship.common import coro, graph_init, graph_prune, graph_save

TYPE_PERSON = "https://www.w3.org/ns/person#Person"
TYPE_POST = "http://www.w3.org/ns/org#Post"
TYPE_ORGANIZATION = "http://www.w3.org/ns/org#Organization"
TYPE_MEMBERSHIP = "http://www.w3.org/ns/org#Membership"


@click.group()
def sync():
    pass


@sync.command("all")
@click.confirmation_option(
    prompt="The specified database will be deleted, is the data backed up?"
)
@coro
@click.pass_context
async def all_sync(ctx):
    graph = graph_init()
    graph.clear()
    graph_save(graph)

    await tree_import(TYPE_PERSON, "Person", person_build_node)
    await tree_import(TYPE_ORGANIZATION, "Organization", organization_build_node)
    await tree_import(TYPE_POST, "Post", post_build_node)
    await tree_import(TYPE_MEMBERSHIP, "Membership", membership_build_node)


@sync.command("membership")
@coro
async def membership():
    await tree_import(TYPE_MEMBERSHIP, "Membership", membership_build_node)


def membership_build_node(membership):
    return (
        attribute_filter_empty(
            {"id": membership["@id"], "label": membership.get("label", None)}
        ),
        relationship_filter_empty(
            [
                {
                    "subject": membership["@id"],
                    "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "object": TYPE_MEMBERSHIP,
                },
                {
                    "subject": membership["@id"],
                    "predicate": "http://www.w3.org/TR/vocab-org/#org:member",
                    "object": get_in(["person", "@id"], membership, None),
                },
                {
                    "subject": membership["@id"],
                    "predicate": "http://www.w3.org/TR/vocab-org/#org:organization",
                    "object": get_in(["organization", "@id"], membership, None),
                },
                {
                    "subject": membership["@id"],
                    "predicate": "http://www.w3.org/ns/opengov#post",
                    "object": get_in(["post", "@id"], membership, None),
                },
                {
                    "subject": membership["@id"],
                    "predicate": "http://www.w3.org/ns/opengov#onBehalfOf",
                    "object": get_in(["on_behalf_of", "@id"], membership, None),
                },
            ]
        ),
    )


@sync.command("post")
@coro
async def post():
    await tree_import(TYPE_POST, "Post", post_build_node)


def post_build_node(post):
    return (
        attribute_filter_empty(
            {"id": post["@id"], "label": post["label"], "role": post.get("role", None)},
        ),
        relationship_filter_empty(
            [
                {
                    "subject": post["@id"],
                    "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "object": TYPE_POST,
                },
                {
                    "subject": post["@id"],
                    "predicate": "http://www.w3.org/TR/vocab-org/#org:organization",
                    "object": get_in(["organization", "@id"], post, None),
                },
            ]
        ),
    )


@sync.command("org")
@coro
async def organization():
    await tree_import(TYPE_ORGANIZATION, "Organization", organization_build_node)


def organization_build_node(organization):
    return (
        attribute_filter_empty(
            {
                "id": organization["@id"],
                "name": organization["name"],
                "classficication": get_in(
                    ["classification", "token"], organization, None
                ),
            },
        ),
        relationship_filter_empty(
            [
                {
                    "subject": organization["@id"],
                    "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "object": TYPE_ORGANIZATION,
                },
                {
                    "subject": organization["@id"],
                    "predicate": "http://www.w3.org/ns/org#org:subOrganizationOf",
                    "object": get_in(
                        ["parent_organization", "@id"], organization, None
                    ),
                },
            ]
        ),
    )


def param_build(portal_type, b_start):
    return {"portal_type": portal_type, "fullobjects": 1, "b_start": b_start}


@sync.command("person")
@coro
async def person():
    await tree_import(TYPE_PERSON, "Person", person_build_node)


def person_build_node(person):
    return (
        attribute_filter_empty(
            {
                "id": person["@id"],
                "name": person["name"],
                "gender": get_in(["gender", "token"], person, None),
                "head_shot": get_in(["image", "download"], person, None),
                "summary": person.get("summary", None),
                "biography": person.get("biography", None),
            },
        ),
        [
            {
                "subject": person["@id"],
                "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "object": TYPE_PERSON,
            }
        ],
    )


def attribute_filter_empty(result):
    return valfilter(lambda x: x is not None, result)


async def fetch(portal_type, session_api, b_start=0, _result=None):
    async with session_api.get(
        os.environ.get("API_ENDPOINT", "https://politikus.sinarproject.org/@search"),
        params=param_build(portal_type, b_start),
    ) as response:
        click.echo(f"Fetching from {response.url}")

        page = await response.json()

        result = (_result or []) + page.get("items", [])

        time.sleep(float(os.environ.get("CRAWL_INTERVAL", 1)))

        return (
            await fetch(
                portal_type,
                session_api,
                parse_qs(urlsplit(page["batching"]["next"]).query)["b_start"][0],
                result,
            )
            if "next" in page.get("batching", {})
            else result
        )


def relationship_filter_empty(result):
    return [
        relationship for relationship in result if relationship["object"] is not None
    ]


def session_start_http():
    return aiohttp.ClientSession(
        headers={"Accept": "application/json"}, json_serialize=ujson.dumps
    )


async def tree_build(portal_type, node_builder, session):
    return reduce(
        lambda current, incoming: dict(
            {},
            nodes=dict(current["nodes"], **{incoming[0]["id"]: incoming[0]}),
            relationships=current["relationships"] + incoming[1],
        ),
        [node_builder(entity) for entity in await fetch(portal_type, session)],
        {"nodes": {}, "relationships": []},
    )


async def tree_import(tree_type, portal_type, node_builder):
    async with session_start_http() as session:
        graph = graph_init()

        graph_prune(graph, tree_type)

        tree_insert(graph, **await tree_build(portal_type, node_builder, session))

        graph_save(graph)


def tree_insert(graph, nodes, relationships):
    for node_id, node in nodes.items():
        graph.add_node(node_id, **node)

    for relationship in relationships:
        graph.add_edge(
            relationship["subject"],
            relationship["object"],
            key=relationship["predicate"],
        )
