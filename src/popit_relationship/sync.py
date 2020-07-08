from urllib.parse import urlsplit, parse_qs
import time

from popit_relationship.common import coro
from toolz.dicttoolz import get_in, valfilter

import aiohttp
import click
import ujson
import os


@click.group()
def sync():
    pass


@sync.command("all")
@click.confirmation_option(
    prompt="The specified database will be deleted, is the data backed up?"
)
def all():
    # delete everything

    # membership
    # organization
    # posts
    pass


@sync.command("person")
@coro
async def person():
    async with aiohttp.ClientSession(
        headers={"Accept": "application/json"}, json_serialize=ujson.dumps
    ) as session:
        for person in await person_fetch(session, 0):
            print(person_build_node(person))


def person_build_node(person):
    return valfilter(
        lambda x: x is not None,
        {
            "id": person.get("@id", None),
            "gender": get_in(["gender", "title"], person, None),
            "head_shot": get_in(["image", "download"], person, None),
            "summary": person.get("summary", None),
            "biography": person.get("biography", None),
        },
    )


async def person_fetch(session_api, b_start=0, _result=None):
    async with session_api.get(
        os.environ.get("API_ENDPOINT", "https://politikus.sinarproject.org/@search"),
        params={"portal_type": "Person", "fullobjects": 1, "b_start": b_start},
    ) as response:
        click.echo(f"Fetching from {response.url}")

        page = await response.json()

        result = (_result or []) + page.get("items", [])

        time.sleep(float(os.environ.get("CRAWL_INTERVAL", 1)))

        return (
            await person_fetch(
                session_api,
                parse_qs(urlsplit(page["batching"]["next"]).query)["b_start"][0],
                result,
            )
            if "next" in page.get("batching", {})
            else result
        )


@sync.command("relationship")
def sync_relationship():
    print("sync relationship")
