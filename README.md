This is a POC
==============

Deprecate networkx version for now as the API endpoint change. 

Installation
============

For Ubuntu: 

After that install the necessary python dependencies
```Shell
pip install -r requirements.txt
```

Usage
=====

Create a configuration file `config.yaml` with the following content. The config is there just in case that you use neo4j on other server. 
```
graph_db: bolt://neo4j:password@localhost:7687
refresh: True
```

There are no parameters needed to run `popit_to_neo4j.py`, just run the script
```Shell
$ python popit_to_neo4j.py
```

