import os
import time
from functools import reduce
from urllib.parse import parse_qs, urlsplit

import aiohttp
import click
import ujson
from loguru import logger
from toolz.dicttoolz import get_in, valfilter

from popit_relationship.common import (
    coro,
    graph_init,
    graph_prune,
    graph_save,
    schema_generate_uri,
)
from popit_relationship.schema.sinar import Ownership, Sinar

TYPE_PERSON = "https://www.w3.org/ns/person#Person"
TYPE_POST = "http://www.w3.org/ns/org#Post"
TYPE_ORGANIZATION = "http://www.w3.org/ns/org#Organization"
TYPE_MEMBERSHIP = "http://www.w3.org/ns/org#Membership"
TYPE_RELATIONSHIP = "http://purl.org/vocab/relationship/Relationship"


@click.group()
def sync():
    pass


@sync.command("all")
@coro
@click.pass_context
async def all_sync(_ctx):
    await tree_import(TYPE_PERSON, "Person", person_build_node)
    await tree_import(TYPE_RELATIONSHIP, "Relationship", relationship_build_node)
    await tree_import(TYPE_ORGANIZATION, "Organization", organization_build_node)
    await tree_import(TYPE_POST, "Post", post_build_node)
    await tree_import(TYPE_MEMBERSHIP, "Membership", membership_build_node)
    await tree_import(
        schema_generate_uri(Sinar.OWNERSHIP, Ownership.OWNERSHIP_OR_CONTROL_STATEMENT),
        "Ownership Control Statement",
        ownership_build_node,
    )


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
                    "predicate": {
                        "key": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        "attributes": {},
                    },
                    "object": TYPE_MEMBERSHIP,
                },
                {
                    "subject": membership["@id"],
                    "predicate": {
                        "key": "http://www.w3.org/ns/org#member",
                        "attributes": {},
                    },
                    "object": get_in(["person", "@id"], membership, None),
                },
                {
                    "subject": membership["@id"],
                    "predicate": {
                        "key": "http://www.w3.org/ns/org#organization",
                        "attributes": {},
                    },
                    "object": get_in(["organization", "@id"], membership, None),
                },
                {
                    "subject": membership["@id"],
                    "predicate": {
                        "key": "http://www.w3.org/ns/opengov#post",
                        "attributes": {},
                    },
                    "object": get_in(["post", "@id"], membership, None),
                },
                {
                    "subject": membership["@id"],
                    "predicate": {
                        "key": "http://www.w3.org/ns/opengov#onBehalfOf",
                        "attributes": {},
                    },
                    "object": get_in(["on_behalf_of", "@id"], membership, None),
                },
            ]
        ),
    )


@sync.command("ownership")
@coro
async def ownership():
    await tree_import(
        schema_generate_uri(Sinar.OWNERSHIP, Ownership.OWNERSHIP_OR_CONTROL_STATEMENT),
        "Ownership Control Statement",
        ownership_build_node,
    )


def ownership_build_node(ownership):
    return (
        None,
        relationship_filter_empty(
            [
                {
                    "subject": get_in(["interestedParty", "@id"], ownership, None),
                    "predicate": {
                        "key": schema_generate_uri(
                            Sinar.OWNERSHIP, Ownership.OWNERSHIP_OR_CONTROL_STATEMENT
                        ),
                        "attributes": predicate_attribute_filter_empty(
                            {
                                "interest_level": get_in(
                                    ["interest_level", "token"], ownership, None
                                ),
                                "interest_type": get_in(
                                    ["interest_type", "token"], ownership, None
                                ),
                            }
                        ),
                    },
                    "object": ownership["bods_subject"]["@id"],
                }
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
                    "predicate": {
                        "key": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        "attributes": {},
                    },
                    "object": TYPE_POST,
                },
                {
                    "subject": post["@id"],
                    "predicate": {
                        "key": "http://www.w3.org/ns/org#organization",
                        "attributes": {},
                    },
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
                    "predicate": {
                        "key": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        "attributes": {},
                    },
                    "object": TYPE_ORGANIZATION,
                },
                {
                    "subject": organization["@id"],
                    "predicate": {
                        "key": "http://www.w3.org/ns/org#subOrganizationOf",
                        "attributes": {},
                    },
                    "object": get_in(
                        ["parent_organization", "@id"], organization, None
                    ),
                },
            ]
        ),
    )


@sync.command("rel")
@coro
async def relationship():
    await tree_import(TYPE_RELATIONSHIP, "Relationship", relationship_build_node)


def relationship_build_node(relationship):
    return (
        None,
        [
            {
                "subject": relationship["relationship_subject"]["@id"],
                "predicate": {
                    "key": TYPE_RELATIONSHIP,
                    "attributes": predicate_attribute_filter_empty(
                        relationship_get_attributes(relationship)
                    ),
                },
                "object": relationship["relationship_object"]["@id"],
            }
        ],
    )


def relationship_get_attributes(relationship):
    result, type_name = {}, get_in(["relationship_type", "token"], relationship, None)

    type_uri = {
        "associate": "http://purl.org/vocab/relationship/collaboratesWith",
        "employer": "http://purl.org/vocab/relationship/employerOf",
        "parent": "http://purl.org/vocab/relationship/parentOf",
        "spouse": "http://purl.org/vocab/relationship/spouseOf",
    }

    if type_name and type_uri.get(type_name, None):
        result = {
            "name": relationship["relationship_type"]["token"],
            "uri": type_uri[type_name],
        }
    elif type_name:
        result = {"name": type_name}
    else:
        result = {}

    return result


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
                "biography": get_in(["biography", "data"], person, None),
            },
        ),
        [
            {
                "subject": person["@id"],
                "predicate": {
                    "key": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "attributes": {},
                },
                "object": TYPE_PERSON,
            }
        ],
    )


def attribute_filter_empty(result):
    return valfilter(lambda x: x is not None, result)


async def fetch(portal_type, session_api, b_start=0, _result=None):
    async with session_api.get(
        os.environ.get("ENDPOINT_API", "https://politikus.sinarproject.org/@search"),
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


async def node_build(node_builder, portal_type, session):
    result = []

    for entity in await fetch(portal_type, session):
        try:
            result.append(node_builder(entity))

        except (KeyError, TypeError) as error:
            from pprint import pformat

            logger.exception(error)
            logger.error(
                "The following entity does not have all required fileds:\n{}",
                pformat(entity),
            )

    return result


def node_is_class(node):
    return node in (
        TYPE_PERSON,
        TYPE_RELATIONSHIP,
        TYPE_ORGANIZATION,
        TYPE_POST,
        TYPE_MEMBERSHIP,
    )


def predicate_attribute_filter_empty(attributes):
    return {key: value for (key, value) in attributes.items() if value}


def relationship_filter_empty(result):
    return [
        relationship
        for relationship in result
        if relationship["object"] is not None and relationship["subject"] is not None
    ]


def session_start_http():
    return aiohttp.ClientSession(
        headers={"Accept": "application/json"},
        json_serialize=ujson.dumps,
        auth=(
            aiohttp.BasicAuth(
                os.environ["API_AUTH_USER"], os.environ.get("API_AUTH_PASS", None)
            )
            if os.environ.get("API_AUTH_USER", False)
            else None
        ),
    )


async def tree_build(portal_type, node_builder, session):
    return reduce(
        lambda current, incoming: dict(
            {},
            nodes=dict(current["nodes"], **{incoming[0]["id"]: incoming[0]})
            if incoming[0]
            else current["nodes"],
            relationships=current["relationships"] + incoming[1],
        ),
        await node_build(node_builder, portal_type, session),
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
            key=relationship["predicate"]["key"],
            **relationship["predicate"]["attributes"],
        )
