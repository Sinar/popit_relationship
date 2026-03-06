from popit_relationship.schema.base import Namespace, Schema


class Ownership(Schema):
    OWNERSHIP_OR_CONTROL_STATEMENT = "ownershipOrControlStatement"


class OCDS(Schema):
    """
    Types
    """

    EXTRACTIVE_CONCESSION = "extractiveConcession"
    ORGANIZATION = "organization"

    """
    Relationships
    """
    BUYER = "buyer"
    PROCURING_ENTITY = "procuringEntity"
    ADMINISTRATIVE_ENTITY = "administrativeEntity"
    TENDERER = "tenderer"
    SUPPLIER = "supplier"
    FUNDER = "funder"
    REVIEW_BODY = "reviewBody"
    INTERESTED_PARTY = "interestedParty"


class Sinar(Namespace):
    BASE = "https://sinarproject.org/ns/"

    OWNERSHIP = "ownership"
    OCDS = "ocds"
