import unittest
from mock import Mock, patch, call
from popit_to_neo4j import PopItToNeo

__author__ = 'lowks'


class MyTestCase(unittest.TestCase):

    @patch("__builtin__.open")
    @patch("py2neo.Graph.delete_all")
    @patch("yaml.load")
    def setUp(self, mock_load_config, mock_delete_all, mock_open):
        mock_open.return_value = "config.yaml"
        self.base_graph = dict(graph_db='graph_db',
                               refresh=True)
        self.statuses = dict(status=['Status1', 'Status2'],
                             motd='I am the MOTD')

        def load_config(filename):
            if "config.yaml" in filename:
                return self.base_graph
            elif "statuses.yaml" in filename:
                return self.statuses

        mock_load_config.side_effect = load_config
        self.popittoneo = PopItToNeo()

    @patch("__builtin__.open")
    @patch("py2neo.Graph.delete_all")
    @patch("yaml.load")
    def test_refresh_during_init(self, mock_load_config,
                                 mock_graph_delete, mock_open):

        """Test to ensure that refresh is called during init"""

        mock_open.return_value = "config.yaml"

        def load_config(filename):
            if "config.yaml" in filename:
                return self.base_graph
            elif "statuses.yaml" in filename:
                return self.statuses

        mock_load_config.side_effect = load_config
        popittoneo = PopItToNeo()
        self.assertTrue(mock_graph_delete.called)

    @patch("popit_to_neo4j.PopItToNeo.fetch_entity")
    @patch("popit_to_neo4j.PopItToNeo.fetch_post")
    @patch("logging.warning")
    @patch("popit_to_neo4j.Graph.create")
    def test_process_post(self, mock_create, mock_warning,
                          mock_fetch_post, mock_fetch_entity):
        mock_fetch_entity.return_value = {"result": [{"id": "node1"}]}
        mock_fetch_post.return_value = "node1"
        self.popittoneo.process_posts()
        call('node1') in mock_create.call_args_list

if __name__ == '__main__':
    unittest.main()
