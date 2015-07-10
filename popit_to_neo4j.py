__author__ = 'sweemeng'
from py2neo import Graph
from py2neo import Node
from py2neo import Relationship
import requests
import time
import logging


class PopItToNeo(object):
    def __init__(self):
        self.endpoint = "https://sinar-malaysia.popit.mysociety.org/api/v0.1"

        # you know so that you can override this. why? I am not sure
        self.membership_field = "memberships"
        self.person_field = "persons"
        self.organization_field = "organizations"
        self.post_field = "posts"
        self.graph = Graph()

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

            entries = data["result"]
            for entry in entries:
                # a membership have 3 important field, person_id, organization_id, posts_id
                if not (entry.get("person_id") and entry.get("organization_id")):
                    continue

                person = self.fetch_person(entry["person_id"])
                role = entry.get("role","member")
                if not role:
                    role = "member"
                logging.warning("Role: %s" % role)
                if entry.get("post_id"):
                    post = self.fetch_post(entry["post_id"])
                    if self.graph.match_one(person, role, post):
                        logging.warning("Already exist, skipping")
                        continue
                    relationship = Relationship(person, role, post)
                else:
                    organization = self.fetch_organization(entry["organization_id"])
                    if self.graph.match_one(person, role, organization):
                        logging.warning("Already exist, skipping")
                        continue
                    relationship = Relationship(person, role, organization)
                self.graph.create(relationship)
            if data.get("next_url"):
                membership_url = data.get("next_url")
            else:
                break

    def fetch_person(self, person_id):
        if person_id in self.person_processed:
            logging.warning("Person %s fetch from cache" % person_id)
            return self.person_processed[person_id]

        node = self.graph.find_one("Persons", "popit_id", person_id)
        if node:
            logging.warning("Already exist, skipping")
            self.person_processed[person_id] = node
            return node

        person_url = "%s/%s/%s" % (self.endpoint, self.person_field, person_id)
        data = self.fetch_entity(person_url)
        logging.warning("Fetching person")

        entity = data["result"]
        if type(entity["name"]) == list:
            name = entity["name"][0]

        else:
            name = entity["name"]
        logging.warning("Name: %s" % name)
        node = Node("Persons", name=name, popit_id=entity["id"])
        self.graph.create(node)
        self.person_processed[entity["id"]] = node
        return node

    def fetch_organization(self, organization_id):
        if organization_id in self.organization_processed:
            logging.warning("Organization %s fetch from cache" % organization_id)
            return self.organization_processed[organization_id]

        node = self.graph.find_one("Organization", "popit_id", organization_id)
        if node:
            logging.warning("Already exist, skipping")
            self.organization_processed[organization_id] = node
            return node

        organization_url = "%s/%s/%s" % (self.endpoint, self.organization_field, organization_id)
        data = self.fetch_entity(organization_url)
        logging.warning("Fetch orgnanization")

        entity = data["result"]
        if type(entity["name"]) == list:
            name = entity["name"][0]
        else:
            name = entity["name"]

        logging.warning("Name: %s" % name)
        if "classification" in entity:

            logging.warning("Classification:%s" % entity["classification"])
            node = Node("Organization", name=name, popit_id=entity["id"], classification=entity["classification"])
        else:
            node = Node("Organization", name=name, popit_id=entity["id"])
        self.graph.create(node)
        self.organization_processed[entity["id"]] = node
        return node

    def fetch_post(self, post_id):
        if post_id in self.post_processed:
            logging.warning("Post %s fetch from cache" % post_id)
            return self.post_processed[post_id]

        node = self.graph.find_one("Posts", "popit_id", post_id)
        if node:
            logging.warning("Already exist, skipping")
            self.post_processed[post_id] = node
            return node

        post_url = "%s/% s/%s" % (self.endpoint, self.post_field, post_id)
        data = self.fetch_entity(post_url)
        logging.warning("Fetch post")

        entity = data["result"]
        # Fetch organization node, because post is link to organization
        # What is the implication of post without organization?
        try:
            if entity.get("organization_id"):
                organization = self.fetch_organization(entity["organization_id"])
            else:
                organization = None
        except Exception as e:
            logging.warning(e.message)
            organization = None
        logging.warning("Label: %s" % entity["label"])
        node = Node("Posts", name=entity["label"], popit_id=entity["id"])
        self.graph.create(node)
        self.post_processed[entity["id"]] = node
        if organization:
            relation = Relationship(node, "of", organization)
            self.graph.create(relation)

        return node

    def process_parent_company(self):
        organizations_url = "%s/%s" % (self.endpoint, self.organization_field)


        while True:
            data = self.fetch_entity(organizations_url)
            entries = data["result"]
            for entry in entries:
                if not entry.get("parent_id"):
                    logging.warning("No parent id, moving on")
                    continue
                else:
                    logging.warning(entry.get("parent_id"))

                test_url = "%s/%s/%s" % (self.endpoint, self.organization_field, entry["parent_id"])
                # TODO: How to refactor this
                test_result = requests.get(test_url)

                if test_result.status_code != 200:
                    logging.warning(test_result.content)
                    continue

                # TODO: Dafuq this is not DRY.

                parent_node = self.fetch_organization(entry["parent_id"])
                child_node = self.fetch_organization(entry["id"])
                parent_relationship = Relationship(parent_node, "parent_of", child_node)
                if self.graph.match_one(parent_node, "parent_of", child_node):
                    logging.warning("relation exist %s %s" % (entry["id"], entry["parent_id"]))
                    continue
                self.graph.create(parent_relationship)
                if self.graph.match_one(child_node, "child_of", parent_node):
                    logging.warning("relation exist %s %s" % (entry["id"], entry["parent_id"]))
                    continue
                child_relationship = Relationship(child_node, "child_of", parent_node)
                self.graph.create(child_relationship)

            if "next_url" in data:
                organizations_url = data["next_url"]
                logging.warning(organizations_url)
            else:
                break

    def process_posts(self):
        post_url = "%s/%s" % (self.endpoint, self.post_field)
        while True:
            data = self.fetch_entity(post_url)
            entries = data["result"]
            for entry in entries:
                node = self.fetch_post(entry["id"])
                self.graph.create(node)
                # Since creating organization relationship is already part of getting post
                # ourjob is done here
            if "next_url" in data:
                post_url = data["next_url"]
                logging.warning(post_url)
            else:
                break

    def fetch_entity(self, url):
        r = requests.get(url)
        time.sleep(0.1)
        if r.status_code != 200:
            raise Exception(r.content)
        return r.json()


if __name__ == "__main__":
    loader = PopItToNeo()
    loader.process_membership()
    loader.process_parent_company()
    loader.process_posts()
