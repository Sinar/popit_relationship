# Popit relationship fetcher and importer

This script pulls data from politikus.sinarproject.org and cache it in networkx to enable offline processing. The cache can then be saved into a Neo4j database for further processing and visualization.

## Building and Installing

### Prerequisites

The project depends on the following tools / python package in order to build and install properly.

1. Python 3.6 and up
1. While the development work targets Neo4j 4.1, earlier version should work.
1. Poetry - follow the installation instruction found [here](https://python-poetry.org/docs/#installation).
1. Python wheel - you can install via pip
   ```
   pip3 install wheel
   ```

### Building

1. Clone this project
   ```
   git clone https://github.com/Sinar/popit_relationship
   cd popit_relationship
   ```
2. Install and build the project
   ```
   poetry build
   ```

### Install

Install the built project with pip (filename of the `.whl` file may vary). Please ensure your `PATH` is configured properly.

```
pip3 install ./dist/popit_relationship-0.1.0-py3-none-any.whl
```

If you are reinstalling after pulling the latest changes, add a `--force-reinstall` flag

```
pip3 install --force-reinstall ./dist/popit_relationship-0.1.0-py3-none-any.whl
```

## Configuration

Most of the configuration is saved within `.env` file, please refer to the `.env.example` for example. Besides `NEO4J_AUTH` and `NEO4J_URI`, the script should work with the default settings.

- `NEO4J_AUTH` stores the username and passsword pair separated by a backslash character `/`, e.g. `neo4j/s0meCompl!catedPassword`
- `NEO4J_URI` stores the URI to the neo4j database, e.g. `bolt:hostname:7687`
- `ENDPOINT_API` stores the ENDPOINT API URI, currently defaulted to `https://politikus.sinarproject.org/@search`, the script should work with other similar APIs
- `CRAWL_INTERVAL` stores the time to wait between every API call (defaulted to `1` second)
- `CACHE_PATH` stores the path to the cache file (defaulted to `./primport-cache.gpickle`)

The configuration environment variables can be overwritten while executing the script (please refer to the usage examples below).

## Usage

After following the installation guide, if the python environment is properly configured, a script named `primport` should be made available. Sub-commands can then be issued for different tasks.

Configuration options can be overriden as environment variables, e.g. when running `primport` in Bash

```
NEO4J_AUTH=neo4j/someOtherPassword primport reset db
```

### Resetting

- `primport reset cache` resets the cache file
- `primport reset db` clears the Neo4j database

### Sync

- `primport sync person` fetches the `Person` API
- `primport sync org` fetches the `Organization` API
- `primport sync post` fetches the `Post` API
- `primport sync membership` fetches the `Membership` relationship API
- `primport sync all` fetches all of the above

### Saving to the database

- `primport save` saves the cached data to the Neo4j database to allow further work.

### Usage without installing the wheel package

- The script can be executed normally as follows
  ```
  git clone https://github.com/Sinar/popit_relationship
  cd popit_relationship
  poetry install
  poetry run python src/popit_relationship/primport.py reset db
  ```
  (Just replace `primport` with `poetry run python src/popit_relationship/primport.py`)

## Testing (WIP)

Test is done through PyTest

```
poetry run pytest
```
