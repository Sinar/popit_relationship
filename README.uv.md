# Popit relationship fetcher and importer with uv

This guide uses only `uv` and `uvx`. It does not require Poetry for building, running, exporting GraphML, or running tests.

## Prerequisites

1. Python 3.9 or newer installed locally
1. `uv` installed from [Astral](https://docs.astral.sh/uv/)
1. Neo4j only if you intend to use `primport save` or `primport reset db`

## One-off usage with uvx

Run the CLI directly from the local checkout without installing it globally:

```bash
git clone https://github.com/Sinar/popit_relationship
cd popit_relationship
uvx --refresh --from . primport --help
```

The `--refresh` flag is recommended while developing locally so `uvx` rebuilds from the current source instead of reusing an older cached build.

## Configuration

The same environment variables used by the Poetry workflow apply here:

- `NEO4J_AUTH` stores the username and password pair as `user/password`
- `NEO4J_URI` stores the Neo4j URI, for example `bolt:hostname:7687`
- `ENDPOINT_API` stores the API endpoint and defaults to `https://politikus.sinarproject.org/@search`
- `CRAWL_INTERVAL` stores the delay between API calls and defaults to `1`
- `CACHE_PATH` stores the cache path and defaults to `./primport-cache.gpickle`
- `GRAPHML_PATH` stores the default GraphML export path and defaults to `./primport-cache.graphml`

## Common commands

Reset the cache:

```bash
uvx --refresh --from . primport reset cache
```

Sync data:

```bash
uvx --refresh --from . primport sync all
```

Save the cached graph to Neo4j:

```bash
uvx --refresh --from . primport save
```

Export the cached graph to GraphML with NetworkX's built-in GraphML writer:

```bash
uvx --refresh --from . primport export graphml ./primport-cache.graphml
```

If `GRAPHML_PATH` is set, you can omit the output path:

```bash
GRAPHML_PATH=./primport-cache.graphml uvx --refresh --from . primport export graphml
```

## Tests with uvx

Run pytest through `uvx` and install the local project into the ephemeral environment:

```bash
uvx --refresh --with . pytest -q tests/test_primport.py
```

This command was exercised against the current source tree and passed in this workspace.

## Building without Poetry

Build a wheel with `uv`:

```bash
uv build --wheel
```

The generated wheel can then be installed with `uv pip` into a virtual environment if needed.
