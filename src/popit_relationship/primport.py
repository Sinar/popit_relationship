import asyncio
import sys

from dotenv import load_dotenv
from popit_relationship.sync import sync
import click


@click.command()
def reset():
    print("reset db")


@click.group()
def app():
    pass


app.add_command(sync)
app.add_command(reset)


def main():
    load_dotenv()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(app())


if __name__ == "__main__":
    main()
