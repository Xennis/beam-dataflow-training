# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os

from apache_beam.io.filesystems import FileSystems

from pipeline.common import testutil
from pipeline.customer import customer


class TestCustomerPipeline(testutil.FileTestCase):

    TESTDATA_DIR = FileSystems.join(os.path.dirname(os.path.realpath(__file__)), 'testdata')

    def test_full(self):
        with testutil.TempDir() as test_dir:
            detail_input = FileSystems.join(test_dir, 'detail.json')
            order_input = FileSystems.join(test_dir, 'order.json')
            FileSystems.copy([
                FileSystems.join(self.TESTDATA_DIR, 'detail.json'),
                FileSystems.join(self.TESTDATA_DIR, 'order.json')
            ], [
                detail_input,
                order_input
            ])

            customer.run([
                '--detail_input', detail_input,
                '--order_input', order_input,
                '--output', FileSystems.join(test_dir, 'output'),
                '--output_aggregation', 'sum'
            ])

            expected_output = [
                '{"errors": [], "first_name": "Bart", "id": 1, "last_name": "Bruck", "sum": "287.69"}\n',
                '{"errors": ["email \'wtuppeny2bandcamp.com\' is invalid"], "first_name": "Winny", "id": 3, "last_name": "Tuppeny", "sum": "1075.90"}\n',  # nopep8, pylint: disable=line-too-long
                '{"errors": ["no customer details"], "first_name": null, "id": 2, "last_name": null, "sum": "2016.75"}\n'  # nopep8, pylint: disable=line-too-long
            ]
            self.assertFileEqual(FileSystems.join(test_dir, 'output-*-of-*.json'), expected_output)

    def test_full_avg(self):
        with testutil.TempDir() as test_dir:
            detail_input = FileSystems.join(test_dir, 'detail.json')
            order_input = FileSystems.join(test_dir, 'order.json')
            FileSystems.copy([
                FileSystems.join(self.TESTDATA_DIR, 'detail.json'),
                FileSystems.join(self.TESTDATA_DIR, 'order.json')
            ], [
                detail_input,
                order_input
            ])

            customer.run([
                '--detail_input', detail_input,
                '--order_input', order_input,
                '--output', FileSystems.join(test_dir, 'output'),
                '--output_aggregation', 'avg'
            ])

            expected_output = [
                u'{"avg": "287.69", "errors": [], "first_name": "Bart", "id": 1, "last_name": "Bruck"}\n',
                u'{"avg": "537.95", "errors": ["email \'wtuppeny2bandcamp.com\' is invalid"], "first_name": "Winny", "id": 3, "last_name": "Tuppeny"}\n',  # nopep8, pylint: disable=line-too-long
                u'{"avg": "672.25", "errors": ["no customer details"], "first_name": null, "id": 2, "last_name": null}\n'  # nopep8, pylint: disable=line-too-long
            ]
            self.assertFileEqual(FileSystems.join(test_dir, 'output-*-of-*.json'), expected_output)
