import click


@click.group()
def main():
    pass


@click.group()
def sync():
    pass


@sync.command("person")
def sync_person():
    print("sync person")
    pass


@sync.command("relationship")
def sync_relationship():
    print("sync relationship")
    pass


main.add_command(sync)

if __name__ == "__main__":
    main()
