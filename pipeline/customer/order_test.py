# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from decimal import Decimal
import os
import unittest

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipeline.customer.order import Field, Parse, Validate, GroupByCustomer, AggregateOrders, Prepare


class TestParse(unittest.TestCase):

    def test_valid(self):
        data = '{"id":880,"customer_id":1,"total_price":"287.69"}'
        parsed_expected = [
            (880, {
                'id': 880,
                'customer_id': 1,
                'total_price': '287.69',
            })
        ]
        broken_expected = []

        actual = [data] | beam.ParDo(Parse()).with_outputs(Parse.TAG_BROKEN_DATA, main='parsed')
        self.assertEqual(parsed_expected, actual['parsed'])
        self.assertEqual(broken_expected, actual[Parse.TAG_BROKEN_DATA])

    def test_id_only(self):
        data = '{"id":880}'
        parsed_expected = [
            (880, {
                'id': 880,
                'customer_id': None,
                'total_price': None,
            })
        ]
        broken_expected = []

        actual = [data] | beam.ParDo(Parse()).with_outputs(Parse.TAG_BROKEN_DATA, main='parsed')
        self.assertEqual(parsed_expected, actual['parsed'])
        self.assertEqual(broken_expected, actual[Parse.TAG_BROKEN_DATA])

    def test_missing_id(self):
        data = '{"customer_id":1,"total_price":"287.69"}'
        parsed_expected = []
        broken_expected = [
            {
                'element': '{"customer_id":1,"total_price":"287.69"}',
                'error': 'id is missing'
            }
        ]

        actual = [data] | beam.ParDo(Parse()).with_outputs(Parse.TAG_BROKEN_DATA, main='parsed')
        self.assertEqual(parsed_expected, actual['parsed'])
        self.assertEqual(broken_expected, actual[Parse.TAG_BROKEN_DATA])

    def test_no_json(self):
        data = 'I am not JSON'
        parsed_expected = []
        broken_expected = [
            {
                'element': 'I am not JSON',
                'error': 'No JSON object could be decoded'
            }
        ]

        actual = [data] | beam.ParDo(Parse()).with_outputs(Parse.TAG_BROKEN_DATA, main='parsed')
        self.assertEqual(parsed_expected, actual['parsed'])
        self.assertEqual(broken_expected, actual[Parse.TAG_BROKEN_DATA])


class TestValidate(unittest.TestCase):

    def test_valid(self):
        data = (880, {
            'id': 880,
            'customer_id': 1,
            'total_price': '287.69',
        })
        expected = [
            (880, {
                'id': 880,
                'customer_id': 1,
                'total_price': Decimal('287.69'),
                'error': [],
            })
        ]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)

    def test_id_only(self):
        data = (880, {
            'id': 880,
            'customer_id': None,
            'total_price': None,
        })
        expected = [
            (880, {
                'id': 880,
                'customer_id': None,
                'total_price': None,
                'error': ['customer id is missing', 'total price is missing'],
            })
        ]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)

    def test_invalid_total_price(self):
        data = (880, {
            'id': 880,
            'customer_id': 1,
            'total_price': 'I am not a price',
        })
        expected = [
            (880, {
                'id': 880,
                'customer_id': 1,
                'total_price': None,
                'error': ['total price \'I am not a price\' is invalid'],
            })
        ]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)

    def test_invalid_total_price_type(self):
        data = (880, {
            'id': 880,
            'customer_id': 1,
            'total_price': True,
        })
        expected = [
            (880, {
                'id': 880,
                'customer_id': 1,
                'total_price': None,
                'error': ['total price \'True\' is invalid'],
            })
        ]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)


class TestGroupByCustomer(unittest.TestCase):

    def test_empty(self):  # pylint: disable=no-self-use
        data = []
        expected = []

        with TestPipeline() as p:
            actual = (
                p
                | beam.Create(data)
                | GroupByCustomer()
            )
            assert_that(actual, equal_to(expected))

    def test_single(self):  # pylint: disable=no-self-use
        data = [(880, {
            'id': 880,
            'customer_id': 1,
            'total_price': '287.69',
        })]
        expected = [(1, [{
            'id': 880,
            'customer_id': 1,
            'total_price': '287.69',
        }])]

        with TestPipeline() as p:
            actual = (
                p
                | beam.Create(data)
                | GroupByCustomer()
            )
            assert_that(actual, equal_to(expected))

    def test_multiple(self):  # pylint: disable=no-self-use
        data = [
            (3607, {
                'id': 3607,
                'customer_id': 3,
                'total_price': '373.02',
            }),
            (3949, {
                'id': 3949,
                'customer_id': 3,
                'total_price': '702.88',
            }),
        ]
        expected = [(3, [
            {
                'id': 3607,
                'customer_id': 3,
                'total_price': '373.02',
            },
            {
                'id': 3949,
                'customer_id': 3,
                'total_price': '702.88',
            },
        ])]

        with TestPipeline() as p:
            actual = (
                p
                | beam.Create(data)
                | GroupByCustomer()
            )
            assert_that(actual, equal_to(expected))

    def test_mixed(self):  # pylint: disable=no-self-use
        data = [
            (880, {
                'id': 880,
                'customer_id': 1,
                'total_price': '287.69',
            }),
            (3607, {
                'id': 3607,
                'customer_id': 3,
                'total_price': '373.02',
            }),
            (3949, {
                'id': 3949,
                'customer_id': 3,
                'total_price': '702.88',
            }),
        ]
        expected = [
            (1, [{
                'id': 880,
                'customer_id': 1,
                'total_price': '287.69',
            }]),
            (3, [
                {
                    'id': 3607,
                    'customer_id': 3,
                    'total_price': '373.02',
                },
                {
                    'id': 3949,
                    'customer_id': 3,
                    'total_price': '702.88',
                },
            ]),
        ]

        with TestPipeline() as p:
            actual = (
                p
                | beam.Create(data)
                | GroupByCustomer()
            )
            assert_that(actual, equal_to(expected))


class TestAggregateOrders(unittest.TestCase):

    def test_empty(self):
        data = (3, [])
        expected = [(3, {
            Field.CustomerID: 3,
            Field.Orders: [],
            Field.Min: None,
            Field.Max: None,
            Field.Sum: None,
            Field.Avg: None,
            Field.Error: [],
        })]
        actual = [data] | beam.ParDo(AggregateOrders())
        self.assertEqual(expected, actual)

    def test_single(self):
        data = (3, [{
            Field.Id: 3607,
            Field.CustomerID: 3,
            Field.TotalPrice: Decimal('373.02'),
        }])
        expected = [(3, {
            Field.CustomerID: 3,
            Field.Orders: [{
                Field.Id: 3607,
                Field.CustomerID: 3,
                Field.TotalPrice: Decimal('373.02'),
            }],
            Field.Min: Decimal('373.02'),
            Field.Max: Decimal('373.02'),
            Field.Sum: Decimal('373.02'),
            Field.Avg: Decimal('373.02'),
            Field.Error: [],
        })]
        actual = [data] | beam.ParDo(AggregateOrders())
        self.assertEqual(expected, actual)

    def test_multiple(self):
        data = (3, [
            {
                Field.Id: 3607,
                Field.CustomerID: 3,
                Field.TotalPrice: Decimal('373.02'),
            },
            {
                Field.Id: 3949,
                Field.CustomerID: 3,
                Field.TotalPrice: Decimal('702.88'),
            },
        ])
        expected = [(3, {
            Field.CustomerID: 3,
            Field.Orders: [
                {
                    Field.Id: 3607,
                    Field.CustomerID: 3,
                    Field.TotalPrice: Decimal('373.02'),
                },
                {
                    Field.Id: 3949,
                    Field.CustomerID: 3,
                    Field.TotalPrice: Decimal('702.88'),
                },
            ],
            Field.Min: Decimal('373.02'),
            Field.Max: Decimal('702.88'),
            Field.Sum: Decimal('1075.90'),
            Field.Avg: Decimal('537.95'),
            Field.Error: [],
        })]
        actual = [data] | beam.ParDo(AggregateOrders())
        self.assertEqual(expected, actual)

    def test_error(self):
        data = (3, [
            {
                Field.Id: 3607,
                Field.CustomerID: 3,
                Field.TotalPrice: None,
                Field.Error: 'total price \'I am not a price\' is invalid',
            },
            {
                Field.Id: 3949,
                Field.CustomerID: 3,
                Field.TotalPrice: Decimal('702.88'),
            },
        ])
        expected = [(3, {
            Field.CustomerID: 3,
            Field.Orders: [
                {
                    Field.Id: 3607,
                    Field.CustomerID: 3,
                    Field.TotalPrice: None,
                    Field.Error: 'total price \'I am not a price\' is invalid',
                },
                {
                    Field.Id: 3949,
                    Field.CustomerID: 3,
                    Field.TotalPrice: Decimal('702.88'),
                },
            ],
            Field.Min: None,
            Field.Max: None,
            Field.Sum: None,
            Field.Avg: None,
            Field.Error: ['total price \'I am not a price\' is invalid'],
        })]
        actual = [data] | beam.ParDo(AggregateOrders())
        self.assertEqual(expected, actual)


class TestPrepare(unittest.TestCase):

    test_data_dir = FileSystems.join(os.path.dirname(os.path.realpath(__file__)), 'testdata')

    def test_valid(self):
        file_pattern = FileSystems.join(self.test_data_dir, 'order.json')
        expected_valid = [
            (880, {
                'id': 880,
                'customer_id': 1,
                'total_price': Decimal('287.69'),
                'error': [],
            }),
            (1342, {
                'id': 1342,
                'customer_id': 2,
                'total_price': Decimal('194.52'),
                'error': [],
            }),
            (1766, {
                'id': 1766,
                'customer_id': 2,
                'total_price': Decimal('985.00'),
                'error': [],
            }),
            (2924, {
                'id': 2924,
                'customer_id': 2,
                'total_price': Decimal('837.23'),
                'error': [],
            }),
            (3607, {
                'id': 3607,
                'customer_id': 3,
                'total_price': Decimal('373.02'),
                'error': [],
            }),
            (3949, {
                'id': 3949,
                'customer_id': 3,
                'total_price': Decimal('702.88'),
                'error': [],
            }),
        ]
        expected_broken = [
            {
                'error': 'id is missing',
                'element': '{"customer_id":3,"total_price":"707.16"}'
            }
        ]
        # Make use of the TestPipeline from the Beam testing util.
        with TestPipeline() as p:
            actual_valid, actual_broken = (
                p | Prepare(file_pattern)
            )
            # The labels are required because otherwise the assert_that Transform does not have a stable unique label.
            assert_that(actual_valid, equal_to(expected_valid), label='valid')
            assert_that(actual_broken, equal_to(expected_broken), label='broken')
