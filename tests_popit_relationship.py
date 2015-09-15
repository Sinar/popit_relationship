import unittest
from mock import Mock, patch, call
from popit_to_neo4j import PopItToNeo
from popit_to_networkx import PopItRelationship

__author__ = 'lowks'


class MyTestCase(unittest.TestCase):

    @patch("popit_to_networkx.nx.MultiDiGraph")
    @patch("__builtin__.open")
    @patch("py2neo.Graph.delete_all")
    @patch("yaml.load")
    def setUp(self, mock_load_config, mock_delete_all, mock_open, mock_multidigraph):
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

        self.graph_patcher = patch("popit_to_networkx.nx.MultiDiGraph")
        self.add_node_patcher = patch("popit_to_networkx.nx.MultiDiGraph.PopItRelationship.add_node")
        self.graph = self.graph_patcher.start()
        self.add_node = self.add_node_patcher.start()

        self.popit2networkx = PopItRelationship()

    def tearDown(self):
        self.graph_patcher.stop()
        self.add_node_patcher.stop()

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

    @patch("popit_to_neo4j.Graph.find_one")
    @patch("popit_to_neo4j.PopItToNeo.fetch_entity")
    @patch("popit_to_neo4j.Node")
    @patch("py2neo.Graph.create")
    def test_fetch_organization(self, mock_create, mock_node, mock_fetch_entity, mock_find_one):
        self.popittoneo.organization_processed = {'1': 'one'}
        self.popittoneo.fetch_organization('1')
        self.assertFalse(mock_find_one.called)
        self.popittoneo.organization_processed.clear()
        mock_find_one.return_value = 'hulahoop'
        self.assertEqual(self.popittoneo.organization_processed, {})
        result = self.popittoneo.fetch_organization('1')
        self.assertEqual(self.popittoneo.organization_processed,
                         {'1': 'hulahoop'})
        self.assertEqual(result, 'hulahoop')

        self.popittoneo.organization_processed.clear()
        mock_find_one.return_value = None
        mock_fetch_entity.return_value = None
        result = self.popittoneo.fetch_organization('1')
        self.assertEqual(mock_fetch_entity.call_args_list,
                         [call('https://sinar-malaysia.popit.mysociety.org/api/v0.1/organizations/1')])
        self.assertIsNone(result)

        mock_result = {"result": {"name": "name",
                                  "id": "id",
                                  "founding_date": "1971-01-01",
                                  "dissolution_date": "2000-01-01",
                                  "classification": "classification"}}
        self.popittoneo.organization_processed.clear()
        mock_find_one.return_value = None
        mock_fetch_entity.return_value = mock_result
        mock_node.return_value = 'hulahoop'
        result = self.popittoneo.fetch_organization('1')
        self.assertEqual(mock_create.call_args_list, [call('hulahoop')])
        self.assertEqual(self.popittoneo.organization_processed,
                         {'id': 'hulahoop'})
        self.assertListEqual(mock_node.call_args_list,
                             [call('Organization', dissolution_date=946656000.0, founding_date=31509000.0,
                                   classification='classification', name='name', popit_id='id')])
        self.assertEqual(result, 'hulahoop')

    def test_networkx_create_persons(self):

        # Test with name as a list type
        data_1 = dict(name=["hulahoop"],
                      id="idhulahoop")
        self.popit2networkx.create_persons(data_1)
        self.assertEqual(self.popit2networkx.colors,
                         {'idhulahoop': 'c'})
        self.assertEqual(self.popit2networkx.labels,
                         {'idhulahoop': ['hulahoop']})

        # Test with name as a non list type

        data_2 = dict(name="hulahoop2",
                      id="idhulahoop2")

        self.popit2networkx.create_persons(data_2)
        self.assertEqual(self.popit2networkx.colors,
                         {'idhulahoop2': 'c',
                          'idhulahoop': 'c'})
        self.assertEqual(self.popit2networkx.labels,
                         {'idhulahoop2': 'hulahoop2',
                          'idhulahoop': ['hulahoop']})

        self.assertEqual(self.graph().add_node.call_args,
                         call('idhulahoop2', name='hulahoop2',
                              entity='persons'))

    def test_networkx_create_organizations(self):

        # Test with name as a list type
        data_1 = dict(name=["hulahoop"],
                      id="idhulahoop")

        self.popit2networkx.create_organizations(data_1)
        self.assertEqual(self.popit2networkx.colors,
                         {'idhulahoop': 'm'})
        self.assertEqual(self.popit2networkx.labels,
                         {'idhulahoop': ['hulahoop']})
        self.assertEqual(self.graph().add_node.call_args,
                         call('idhulahoop',
                              name='hulahoop',
                              classification='generic',
                              entity='organizations'))

        # Test with name as a non list type

        data_2 = dict(name="hulahoop2",
                      id="idhulahoop2",
                      classification="hulahoop2classifcation")

        self.popit2networkx.create_organizations(data_2)
        self.assertEqual(self.popit2networkx.colors,
                         {'idhulahoop2': 'm',
                          'idhulahoop': 'm'})
        self.assertEqual(self.popit2networkx.labels,
                         {'idhulahoop2': 'hulahoop2',
                          'idhulahoop': ['hulahoop']})
        self.assertEqual(self.graph().add_node.call_args,
                         call('idhulahoop2',
                              name='hulahoop2',
                              classification='hulahoop2classifcation',
                              entity='organizations'))

    def test_networkx_create_posts(self):
        data_1 = dict(name=["hulahoop"],
                      id="idhulahoop",
                      label="labelhulahoop")
        self.popit2networkx.create_posts(data_1)
        self.assertEqual(self.graph().add_node.call_args,
                         call('idhulahoop',
                              name='labelhulahoop',
                              entity='posts'))
        self.graph().add_node.reset_mock()
        data_2 = data_1
        data_2["organization_id"] = "orgidhulahoop"
        self.popit2networkx.create_posts(data_2)
        self.assertEqual(self.graph().add_edge.call_args,
                         call('idhulahoop', 'orgidhulahoop',
                              relationship='of'))
        self.assertEqual(self.popit2networkx.labels,
                         {'idhulahoop': 'labelhulahoop'})
        self.assertEqual(self.popit2networkx.colors,
                         {'idhulahoop': 'y'})

    def test_networkx_create_membership(self):
        data_empty = dict(id="hulahoopid")
        self.popit2networkx.create_membership(data_empty)
        self.assertFalse(self.graph().add_edge.called)

        data1 = dict(id="hulahoopid",
                     person_id="personid",
                     organization_id="orgid",
                     post_id="postid")
        self.popit2networkx.create_membership(data1)
        self.assertEqual(self.graph().add_edge.call_args,
                         call('personid', 'postid', role='member'))

        data2 = dict(id="hulahoopid",
                     person_id="personid",
                     organization_id="orgid",
                     role="pig minister")
        self.popit2networkx.create_membership(data2)
        self.assertEqual(self.graph().add_edge.call_args,
                         call('personid', 'orgid', role='pig minister'))

    @patch("popit_to_networkx.nx.write_gpickle")
    @patch("pickle.dump")
    def test_networkx_save_data(self, mock_dump, mock_write_gpickle):
        # Just assign graph a value to make sure it's passed in
        from mock import mock_open
        m_file = mock_open()

        called_with = []

        def dump_side_effect(*args, **kwargs):
            if args[0] == self.popit2networkx.colors:
                called_with.append("colors")
            elif args[0] == self.popit2networkx.labels:
                called_with.append("labels")

        with patch("__builtin__.open", m_file):
            mock_dump.side_effect = dump_side_effect
            self.popit2networkx.graph = "hulahoop"
            self.popit2networkx.colors = {'color1', 'color2'}
            self.popit2networkx.labels = {'label1', 'label2'}
            self.popit2networkx.save_data()
            self.assertEqual(mock_write_gpickle.call_args,
                             call('hulahoop',
                                  'popitgraph.pickle'))
            self.assertTrue(m_file.called)
            self.assertEqual(m_file.call_args,
                             call('node_label.pickle', 'w'))
            self.assertListEqual(sorted(called_with),
                                 ["colors", "labels"])

if __name__ == '__main__':
    unittest.main()
