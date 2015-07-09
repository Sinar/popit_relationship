__author__ = 'sweemeng'
import requests
import networkx as nx
import logging
import time
import pickle


class PopItRelationship(object):
    def __init__(self):
        self.endpoint = "https://sinar-malaysia.popit.mysociety.org/api/v0.1"
        self.entities = ["persons", "organizations", "posts"]
        self.memberships = "memberships"
        self.graph = nx.MultiDiGraph()
        self.labels = {}
        self.colors = {}

    def create_persons(self, data):
        # Use ID because membership refers to id
        logging.warning("processing persons %s" % data["id"])
        if type(data["name"]) == list:
            name = data["name"][0]
        else:
            name = data["name"]
        self.graph.add_node(data["id"], name=name, entity="persons")
        self.labels[data["id"]] = data["name"]
        self.colors[data["id"]] = "c"

    def create_organizations(self, data):
        # use ID because membership and post refer too id
        logging.warning("processing organizations %s" % data["id"])
        if type(data["name"]) == list:
            name = data["name"][0]
        else:
            name = data["name"]
        self.graph.add_node(data["id"], name=name, entity="organizations",
                            classification=data.get("classification", "generic"))
        self.labels[data["id"]] = data["name"]
        self.colors[data["id"]] = "m"

    def create_posts(self, data):
        logging.warning("processing posts %s" % data["id"])
        self.graph.add_node(data["id"], name=data["label"], entity="posts")
        if data.get("organization_id"):
            self.graph.add_edge(data["id"], data["organization_id"], relationship="of")
        self.labels[data["id"]] = data["label"]
        self.colors[data["id"]] = "y"

    def create_membership(self, data):
        logging.warning("processing membership %s" % data["id"])
        if not data.get("person_id"):
            return
        if not data.get("organization_id"):
            return

        if data.get("post_id"):
            self.graph.add_edge(data["person_id"], data["post_id"], role=data.get("role", "member"))
        else:
            self.graph.add_edge(data["person_id"], data["organization_id"], role=data.get("role", "member"))

    def fetch_data(self, entity):
        url = "%s/%s" % (self.endpoint, entity)

        while True:
            logging.warning(url)
            r = requests.get(url)
            if r.status_code != 200:
                raise Exception(r.content)
            data = r.json()
            yield data["result"]

            if data.get("next_url"):
                url = data["next_url"]
            else:
                break
            time.sleep(0.1)

    def build_graph(self):
        for entity in self.entities:
            for entries in self.fetch_data(entity):
                create_func = getattr(self, "create_%s" % entity)
                for entry in entries:
                    create_func(entry)
        for entries in self.fetch_data(self.memberships):
            for entry in entries:
                self.create_membership(entry)
        logging.warning("completed")

    def save_data(self):
        nx.write_gpickle(self.graph, "popitgraph.pickle")
        f = open("node_color.pickle", "w")
        pickle.dump(self.colors, f)
        f.close()
        f = open("node_label.pickle", "w")
        pickle.dump(self.labels, f)
        f.close()

    def load_data(self):
        self.graph = nx.read_gpickle("popitgraph.pickle")
        self.labels = pickle.load(open("node_label.pickle"))
        self.colors = pickle.load(open("node_color.pickle"))

