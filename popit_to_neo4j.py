__author__ = 'sweemeng'
from py2neo import Graph
from py2neo import Node
from py2neo import Relationship
import requests
import time
import datetime
from dateutil.parser import parse
import logging
import yaml
import re

# Because a bulk of history started in 1945 after WW2. It's complicated
DEFAULT_DATE = datetime.date(1945,1,1)

class PopItToNeo(object):
    def __init__(self):
        config = yaml.load(open("config.yaml"))
        self.endpoint = "https://api.popit.sinarproject.org/en"

        # you know so that you can override this. why? I am not sure
        self.membership_field = "memberships"
        self.person_field = "persons"
        self.organization_field = "organizations"
        self.post_field = "posts"
        self.graph = Graph(config["graph_db"])
        if config["refresh"] == True:
            self.graph.delete_all()

        # Because I am still not familiar to query with cypher
        # So lets cache here. Hopefully the memory usage don't kill me
        self.organization_processed = {}
        self.person_processed = {}
        self.post_processed = {}

    def process_membership(self):
        # So lets start from membership
        membership_url = "%s/%s" % (self.endpoint, self.membership_field)
        while True:
            logging.warning("Processing %s" % membership_url)
            data = self.fetch_entity(membership_url)
            logging.warning("Processing membership")

            entries = data["results"]
            for entry in entries:

                # a membership have 3 important field, person_id, organization_id, posts_id
                if not (entry.get("person") and entry.get("organization")):
                    continue

                person = self.fetch_person(entry["person"])
                if not person:
                    continue
                role = entry.get("role","member")
                if not role:
                    role = "member"
                logging.warning("Role: %s" % role)

                params = []

                # This happens only once anyway
                kwparams = {}
                kwparams["popit_id"] = entry["id"]
                start_date = get_timestamp(entry.get("start_date"))
                if start_date:
                    kwparams["start_date"] = start_date
                end_date = get_timestamp(entry.get("end_date"))
                if end_date:
                    kwparams["end_date"] = end_date

                post_exist = False
                if entry.get("post"):
                    post = self.fetch_post(entry["post"])
                    if not post:
                        continue
                    if self.relationship_exist(person, role, post):
                        post_exist = True
                        logging.warning("Already exist, skipping")

                    if not post_exist:

                        relationship = Relationship(person, role, post, **kwparams)
                        self.graph.create(relationship)

                organization_exist = False

                if entry.get("organization"):
                    organization = self.fetch_organization(entry["organization"])
                    if not organization:
                        continue
                    if self.relationship_exist(person, role, organization):
                        logging.warning("Already exist, skipping")
                        organization_exist = True

                    if not organization_exist:

                        relationship = Relationship(person, role, organization, **kwparams)
                        self.graph.create(relationship)

            if data.get("next"):
                membership_url = data.get("next")
            else:
                break

    def fetch_person(self, person_id):
        if person_id in self.person_processed:
            logging.warning("Person %s fetch from cache" % person_id)
            return self.person_processed[person_id]

        node = self.entity_exist("Persons", person_id)
        if node:
            logging.warning("Already exist, skipping")
            self.person_processed[person_id] = node
            return node

        person_url = "%s/%s/%s" % (self.endpoint, self.person_field, person_id)
        data = self.fetch_entity(person_url)
        if not data:
            # Don't assume that this id won't be created the next time
            logging.warning("person not exist %s" % person_id)
            return None
        logging.warning("Fetching person")

        entity = data["result"]
        if type(entity["name"]) == list:
            name = entity["name"][0]

        else:
            name = entity["name"]
        logging.warning("Name: %s" % name)
        kwparam = {}

        birth_date = get_timestamp(entity.get("birth_date"))
        if birth_date:
            kwparam["birth_date"] = birth_date
        death_date = get_timestamp(entity.get("death_date"))
        if death_date:
            kwparam["death_date"] = death_date
        kwparam["name"] = name
        kwparam["popit_id"] = entity["id"]
        node = Node("Persons", **kwparam)
        self.graph.create(node)
        self.person_processed[entity["id"]] = node
        return node

    def fetch_organization(self, organization_id):
        if organization_id in self.organization_processed:
            logging.warning("Organization %s fetch from cache" % organization_id)
            return self.organization_processed[organization_id]

        node = self.entity_exist("Organization", organization_id)
        if node:
            logging.warning("Already exist, skipping")
            self.organization_processed[organization_id] = node
            return node

        organization_url = "%s/%s/%s" % (self.endpoint, self.organization_field, organization_id)
        data = self.fetch_entity(organization_url)
        if not data:
            logging.warning("Organization don't exist %s" % organization_id)
            return None
        logging.warning("Fetch orgnanization")

        entity = data["result"]
        if type(entity["name"]) == list:
            name = entity["name"][0]
        else:
            name = entity["name"]

        kwparams = {}
        logging.warning("Name: %s" % name)
        kwparams["name"] = name
        kwparams["popit_id"] = entity["id"]
        founding_date = get_timestamp(entity.get("founding_date"))
        if founding_date:
            kwparams["founding_date"] = founding_date
        dissolution_date = get_timestamp(entity.get("dissolution_date"))
        if dissolution_date:
            kwparams["dissolution_date"] = dissolution_date

        if "classification" in entity:

            logging.warning("Classification:%s" % entity["classification"])
            kwparams["classification"] = entity["classification"]

        node = Node("Organization", **kwparams)
        self.graph.create(node)
        self.organization_processed[entity["id"]] = node
        return node

    def fetch_post(self, post_id):
        if post_id in self.post_processed:
            logging.warning("Post %s fetch from cache" % post_id)
            return self.post_processed[post_id]

        node = self.entity_exist("Posts", post_id)
        if node:
            logging.warning("Already exist, skipping")
            self.post_processed[post_id] = node
            return node

        post_url = "%s/% s/%s" % (self.endpoint, self.post_field, post_id)
        data = self.fetch_entity(post_url)
        if not data:
            logging.warning("Post don't exist %s" % post_id)
            return None
        logging.warning("Fetch post")

        entity = data["result"]
        # Fetch organization node, because post is link to organization
        # What is the implication of post without organization?
        try:
            if entity.get("organization"):
                organization = self.fetch_organization(entity["organization"])
            else:
                organization = None
        except Exception as e:
            logging.warning(e)
            organization = None
        logging.warning("Label: %s" % entity["label"])
        kwparams = {}
        kwparams["name"] = entity["label"]
        kwparams["popit_id"] = entity["id"]
        start_date = get_timestamp(entity.get("start_date"))
        if start_date:
            kwparams["start_date"] = start_date

        end_date = get_timestamp(entity.get("end_date"))
        if end_date:
            kwparams["end_date"] = end_date

        node = Node("Posts", **kwparams)
        self.graph.create(node)
        self.post_processed[entity["id"]] = node
        if organization:
            temp_param = {}
            if start_date:
                temp_param["start_date"] = start_date
            if end_date:
                temp_param["end_date"] = end_date
            relation = Relationship(node, "of", organization, **kwparams)
            self.graph.create(relation)

        return node

    def process_parent_company(self):
        organizations_url = "%s/%s" % (self.endpoint, self.organization_field)


        while True:
            data = self.fetch_entity(organizations_url)

            entries = data["results"]
            for entry in entries:
                if not entry.get("parent_id"):
                    logging.warning("No parent id, moving on")
                    continue
                else:
                    logging.warning(entry.get("parent_id"))

                # TODO: Dafuq this is not DRY.
                parent_node = self.fetch_organization(entry["parent_id"])
                if not parent_node:
                    continue
                child_node = self.fetch_organization(entry["id"])
                parent_relationship = Relationship(parent_node, "parent_of", child_node)
                if self.relationship_exist(parent_node, "parent_of", child_node):
                    logging.warning("relation exist %s %s" % (entry["id"], entry["parent_id"]))
                    continue
                self.graph.create(parent_relationship)
                if self.relationship_exist(child_node, "child_of", parent_node):
                    logging.warning("relation exist %s %s" % (entry["id"], entry["parent_id"]))
                    continue
                child_relationship = Relationship(child_node, "child_of", parent_node)
                self.graph.create(child_relationship)

            if data.get("next"):
                organizations_url = data["next"]
                logging.warning(organizations_url)
            else:
                break

    def process_posts(self):
        post_url = "%s/%s" % (self.endpoint, self.post_field)
        while True:
            data = self.fetch_entity(post_url)
            entries = data["results"]
            for entry in entries:
                node = self.fetch_post(entry["id"])
                self.graph.create(node)
                # Since creating organization relationship is already part of getting post
                # ourjob is done here
            if data.get("next"):
                post_url = data["next"]
                logging.warning(post_url)
            else:
                break

    def fetch_entity(self, url):
        r = requests.get(url)
        time.sleep(0.1)
        if r.status_code != 200:
            # Just to make output consistent, excception did not kill the script anyway
            return {}
        return r.json()

    def entity_exist(self, entity, popit_id):
        nodes = self.graph.nodes
        node = nodes.match(entity, popit_id=popit_id)
        if node:
            return node.first()
        return None

    def relationship_exist(self, source_entity, relationship, target_entity):
        relationships = self.graph.match_one([source_entity, target_entity], r_type=relationship)
        if relationships:
            return relationships
        return None

def get_timestamp(timestr):
    timestamp = None
    logging.warn(timestr)
    if not timestr:
        return timestamp
    if re.match(r"^0000", timestr):
        return timestamp
    if re.match(r"^9999", timestr):
        return timestamp
    pattern = re.match(r"\d{4}(\-\d{1,2}\-\d{1,2})*", timestr)
    if not pattern:
        return timestamp
    try:
        start_date = parse(pattern.group(), default=DEFAULT_DATE)
    except ValueError as e:
        return timestamp

    epoch = datetime.date(1970, 1, 1)
    diff = epoch - start_date
    timestamp = diff.total_seconds()
    return timestamp


if __name__ == "__main__":
    loader = PopItToNeo()
    loader.process_membership()
    loader.process_parent_company()
    loader.process_posts()
