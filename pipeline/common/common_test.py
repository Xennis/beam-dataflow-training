# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from decimal import Decimal
import json
import unittest

from pipeline.common.common import PipelineJSONEncoder


class TestPipelineJSONEncoder(unittest.TestCase):

    def assert_json(self, expected, obj):
        """assert_json asserts that the given argument is serialised to the expected string."""
        result = json.dumps(obj, cls=PipelineJSONEncoder, sort_keys=True)
        self.assertEqual(expected, result)

    def test_default(self):
        obj = {
            'string': "foobar",
            'int': 42,
            'bool': True,
            'null': None,
        }
        expected = '{"bool": true, "int": 42, "null": null, "string": "foobar"}'
        self.assert_json(expected, obj)

    def test_decimal(self):
        obj = {
            'int': Decimal(200),
            'float': Decimal('9.3333'),
        }
        expected = '{"float": "9.3333", "int": "200"}'
        self.assert_json(expected, obj)
