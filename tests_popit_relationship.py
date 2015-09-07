import unittest
import requests_mock
from mock import Mock, patch, call
from popit_to_neo4j import PopItToNeo

__author__ = 'lowks'


class MyTestCase(unittest.TestCase):

    @patch("__builtin__.open")
    @patch("yaml.load")
    def setUp(self, mock_load_config, mock_open):
        mock_open.return_value = "config.yaml"
        self.base_graph = dict(graph_db='graph_db',
                               refresh=True)
        self.statuses = dict(status=['Status1', 'Status2'],
                             motd='I am the MOTD')

        def load_config(filename):
            if "config.yaml" in filename:
                return self.base_auth
            elif "statuses.yaml" in filename:
                return self.statuses

        mock_load_config.side_effect = load_config

    @patch("__builtin__.open")
    @patch("py2neo.Graph.delete_all")
    @patch("yaml.load")
    def test_refresh_during_init(self, mock_load_config,
                                 mock_graph_delete, mock_open):
        mock_open.return_value = "config.yaml"

        def load_config(filename):
            if "config.yaml" in filename:
                return self.base_graph
            elif "statuses.yaml" in filename:
                return self.statuses

        mock_load_config.side_effect = load_config
        popittoneo = PopItToNeo()
        self.assertTrue(mock_graph_delete.called)
        for attr in ('organization_processed', 'person_processed',
                     'post_processed'):
            self.assertEqual(getattr(popittoneo, attr), {})

    @patch("__builtin__.open")
    @patch("py2neo.Graph.delete_all")
    @patch("yaml.load")
    @patch("popit_to_neo4j.requests.get")
    def test_fetch_entity_status_400(self, mock_get, mock_load,
                                     mock_delete_all, mock_open):
            mock_open.return_value = "config.yaml"

            def load_config(filename):
                if "config.yaml" in filename:
                    return self.base_graph
                elif "statuses.yaml" in filename:
                    return self.statuses

            mock_load.side_effect = load_config
            mock_get.return_value.status_code = 400
            popittoneo = PopItToNeo()
            data = popittoneo.fetch_entity("http://www.google.com")
            self.assertEqual(data, {})

    @patch("__builtin__.open")
    @patch("py2neo.Graph.delete_all")
    @patch("yaml.load")
    @patch("popit_to_neo4j.requests.get")
    def test_fetch_entity_status_200(self, mock_get, mock_load,
                                     mock_delete_all, mock_open):
            mock_open.return_value = "config.yaml"

            def load_config(filename):
                if "config.yaml" in filename:
                    return self.base_graph
                elif "statuses.yaml" in filename:
                    return self.statuses

            mock_load.side_effect = load_config
            mock_get.return_value.status_code = 200

            def return_json():
                return "hulahoop"

            mock_get.return_value.json.side_effect = return_json
            popittoneo = PopItToNeo()
            data = popittoneo.fetch_entity("http://www.google.com")
            self.assertEqual(data, "hulahoop")

    @patch("time.mktime")
    def test_get_timestamp(self, mock_mktime):
        from popit_to_neo4j import get_timestamp
        mock_mktime.return_value = "hulahoop"
        for invalid_input in ('', 'hulahoop', '00000', '4-5-6'):
            self.assertIsNone(get_timestamp(invalid_input))
        data = get_timestamp('2015-09-07')
        self.assertTrue(mock_mktime.called)
        self.assertEqual(data, 'hulahoop')


if __name__ == '__main__':
    unittest.main()
