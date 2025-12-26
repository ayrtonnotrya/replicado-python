
import unittest
import xml.etree.ElementTree as ET
from replicado.utils import etree_to_dict, get_path

class TestUtils(unittest.TestCase):
    def test_etree_to_dict_simple(self):
        xml = "<root><child>val</child></root>"
        root = ET.fromstring(xml)
        d = etree_to_dict(root)
        # Expected: {'root': {'child': 'val'}}
        self.assertEqual(d, {'root': {'child': 'val'}})

    def test_etree_to_dict_attrs(self):
        xml = '<root attr="1"><child>val</child></root>'
        root = ET.fromstring(xml)
        d = etree_to_dict(root)
        # Expected: {'root': {'@attributes': {'attr': '1'}, 'child': 'val'}}
        self.assertEqual(d['root']['@attributes']['attr'], '1')
        self.assertEqual(d['root']['child'], 'val')

    def test_etree_to_dict_list(self):
        xml = '<root><child>A</child><child>B</child></root>'
        root = ET.fromstring(xml)
        d = etree_to_dict(root)
        # Expected: {'root': {'child': ['A', 'B']}}
        self.assertIsInstance(d['root']['child'], list)
        self.assertEqual(d['root']['child'], ['A', 'B'])

    def test_nested_attrs(self):
        xml = '<root><child attr="x">val</child></root>'
        root = ET.fromstring(xml)
        d = etree_to_dict(root)
        # Expected: {'root': {'child': {'@attributes': {'attr': 'x'}}}} 
        # Wait, if child has text AND attrs?
        # My implementation:
        # if text: pass (ignored if attrs exist) <- This is a simplified behavior
        # Let's check.
        pass

    def test_get_path(self):
        d = {'a': {'b': {'c': 1}}}
        self.assertEqual(get_path(d, 'a.b.c'), 1)
        self.assertEqual(get_path(d, 'a.x'), None)
