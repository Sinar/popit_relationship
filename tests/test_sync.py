import ujson

from popit_relationship import sync


def test_person_build_node():
    person = ujson.loads(
        """
        {
          "@components": {
            "actions": {
              "@id": "https://politikus.sinarproject.org/persons/mohammed-azhar-bin-osman-khairuddin/@actions"
            },
            "breadcrumbs": {
              "@id": "https://politikus.sinarproject.org/persons/mohammed-azhar-bin-osman-khairuddin/@breadcrumbs"
            },
            "navigation": {
              "@id": "https://politikus.sinarproject.org/persons/mohammed-azhar-bin-osman-khairuddin/@navigation"
            },
            "types": {
              "@id": "https://politikus.sinarproject.org/persons/mohammed-azhar-bin-osman-khairuddin/@types"
            },
            "workflow": {
              "@id": "https://politikus.sinarproject.org/persons/mohammed-azhar-bin-osman-khairuddin/@workflow"
            }
          },
          "@id": "https://politikus.sinarproject.org/persons/mohammed-azhar-bin-osman-khairuddin",
          "@type": "Person",
          "UID": "5e1ea71edbcd4d36866ffe8e75d6d7b6",
          "allow_discussion": false,
          "biography": null,
          "birth_date": null,
          "contributors": [],
          "created": "2020-06-07T13:04:38+00:00",
          "creators": [
            "kaeru"
          ],
          "death_date": null,
          "effective": "2020-06-07T13:04:42",
          "exclude_from_nav": false,
          "expires": null,
          "gender": {
            "title": "Male",
            "token": "male"
          },
          "id": "mohammed-azhar-bin-osman-khairuddin",
          "image": null,
          "is_folderish": true,
          "language": {
            "title": "English (United Kingdom)",
            "token": "en-gb"
          },
          "layout": "person-view",
          "modified": "2020-06-07T13:04:42+00:00",
          "name": "Mohammed Azhar bin Osman Khairuddin",
          "notes": null,
          "parent": {
            "@id": "https://politikus.sinarproject.org/persons",
            "@type": "Folder",
            "description": "Persons of Interest",
            "review_state": "published",
            "title": "Persons"
          },
          "review_state": "published",
          "rights": null,
          "subjects": [],
          "summary": "Mohammed Azhar bin Osman Khairuddin former director of Suria Strategic Energy Resources Sdn Bhd (SSER)",
          "version": "current"
        }
        """
    )

    expected = (
        {
            "id": "https://politikus.sinarproject.org/persons/mohammed-azhar-bin-osman-khairuddin",
            "name": "Mohammed Azhar bin Osman Khairuddin",
            "gender": "male",
            "summary": "Mohammed Azhar bin Osman Khairuddin former director of Suria Strategic Energy Resources Sdn Bhd (SSER)",
        },
        [
            {
                "subject": "https://politikus.sinarproject.org/persons/mohammed-azhar-bin-osman-khairuddin",
                "predicate": {
                    "key": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "attributes": {},
                },
                "object": "https://www.w3.org/ns/person#Person",
            }
        ],
    )

    assert sync.person_build_node(person) == expected
