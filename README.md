This is a POC
==============

The goal of this project is to feed popit data to a do network analysis. 

There is 2 version of the script, one to generate data in networkx, the other feed data into neo4j. 

Installation
============

For Ubuntu: 

Firstly install graphviz and libgrapviz
```Shell
sudo apt-get install graphviz libgraphviz-dev
```

After that install the necessary python dependencies
```Shell
pip install -r requirements.txt
```

There are no parameters needed to run `popit_to_neo4j.py`, just run the script
```Shell
$ python popit_to_neo4j.py
```

For `popit_to_networkx.py`, i did not make a complete solution
```python
import networkx as nx
import matplotlib as plt
from popit_to_networkx import PopItRelationship

p = PopItRelationship()
p.build_graph()
nx.draw(p)
```

Will add more documentation, and more code as I hack around
