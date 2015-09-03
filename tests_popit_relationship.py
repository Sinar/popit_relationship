__author__ = 'lowks'

import unittest
from mock import Mock, patch, call
from popit_to_neo4j import PopItToNeo


class MyTestCase(unittest.TestCase):

    @patch("__builtin__.open")
    @patch("yaml.load")
    def setUp(self, mock_load_config, mock_open):
        mock_open.return_value = "config.yaml"
        self.base_graph = dict(graph_db='graph_db',
                               refresh=True)
        self.statuses = dict(status=['Status1', 'Status2'], motd='I am the MOTD')
        def load_config(filename):
            if "config.yaml" in filename:
                return self.base_auth
            elif "statuses.yaml" in filename:
                return self.statuses
        mock_load_config.side_effect = load_config

    @patch("__builtin__.open")
    @patch("py2neo.Graph.delete_all")
    @patch("yaml.load")
    def test_refresh_during_init(self, mock_load_config, mock_graph_delete, mock_open):
        mock_open.return_value = "config.yaml"
        def load_config(filename):
            if "config.yaml" in filename:
                return self.base_graph
            elif "statuses.yaml" in filename:
                return self.statuses
        mock_load_config.side_effect = load_config
        popittoneo = PopItToNeo()
        self.assertTrue(mock_graph_delete.called)


if __name__ == '__main__':
    unittest.main()
