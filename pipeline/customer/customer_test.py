# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from decimal import Decimal
import unittest

import apache_beam as beam

from pipeline.customer import customer, detail, order
from pipeline.customer.customer import Validate, JsonFormatter


class TestValidate(unittest.TestCase):

    def test_empty(self):
        data = (3, {
            customer.Field.Detail: [],
            customer.Field.Orders: [],
        })
        expected = [(3, {
            customer.Field.CustomerId: 3,
            customer.Field.Detail: None,
            customer.Field.Orders: None,
            customer.Field.Errors: ['no customer details'],
        })]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)

    def test_no_details(self):
        data = (3, {
            customer.Field.Detail: [],
            customer.Field.Orders: [{
                order.Field.CustomerID: 3,
                order.Field.Orders: [
                    {
                        order.Field.Id: 3607,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('373.02'),
                    },
                    {
                        order.Field.Id: 3949,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('702.88'),
                    },
                ],
                order.Field.Min: Decimal('373.02'),
                order.Field.Max: Decimal('702.88'),
                order.Field.Sum: Decimal('1075.90'),
                order.Field.Avg: Decimal('537.95'),
                order.Field.Error: [],
            }],
        })
        expected = [(3, {
            customer.Field.CustomerId: 3,
            customer.Field.Detail: None,
            customer.Field.Orders: {
                order.Field.CustomerID: 3,
                order.Field.Orders: [
                    {
                        'customer_id': 3,
                        'id': 3607,
                        'total_price': Decimal('373.02'),
                    },
                    {
                        'customer_id': 3,
                        'id': 3949,
                        'total_price': Decimal('702.88'),
                    }],
                order.Field.Min: Decimal('373.02'),
                order.Field.Max: Decimal('702.88'),
                order.Field.Sum: Decimal('1075.90'),
                order.Field.Avg: Decimal('537.95'),
                order.Field.Error: [],
            },
            customer.Field.Errors: ['no customer details'],
        })]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)

    def test_no_orders(self):
        data = (3, {
            customer.Field.Detail: [{
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny@bandcamp.com',
            }],
            customer.Field.Orders: [],
        })
        expected = [(3, {
            customer.Field.CustomerId: 3,
            customer.Field.Detail: {
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny@bandcamp.com',
            },
            customer.Field.Orders: None,
            customer.Field.Errors: [],
        })]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)

    def test_ok(self):
        data = (3, {
            customer.Field.Detail: [{
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny@bandcamp.com',
            }],
            customer.Field.Orders: [{
                order.Field.CustomerID: 3,
                order.Field.Orders: [
                    {
                        order.Field.Id: 3607,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('373.02'),
                    },
                    {
                        order.Field.Id: 3949,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('702.88'),
                    },
                ],
                order.Field.Min: Decimal('373.02'),
                order.Field.Max: Decimal('702.88'),
                order.Field.Sum: Decimal('1075.90'),
                order.Field.Avg: Decimal('537.95'),
                order.Field.Error: [],
            }],
        })
        expected = [(3, {
            customer.Field.CustomerId: 3,
            customer.Field.Detail: {
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny@bandcamp.com',
            },
            customer.Field.Orders: {
                order.Field.CustomerID: 3,
                order.Field.Orders: [
                    {
                        order.Field.Id: 3607,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('373.02'),
                    },
                    {
                        order.Field.Id: 3949,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('702.88'),
                    }],
                order.Field.Min: Decimal('373.02'),
                order.Field.Max: Decimal('702.88'),
                order.Field.Sum: Decimal('1075.90'),
                order.Field.Avg: Decimal('537.95'),
                order.Field.Error: [],
            },
            customer.Field.Errors: [],
        })]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)

    def test_detail_error(self):
        data = (3, {
            customer.Field.Detail: [{
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny2bandcamp.com',
                detail.Field.Error: ['email \'wtuppeny2bandcamp.com\' is invalid'],
            }],
            customer.Field.Orders: [{
                order.Field.CustomerID: 3,
                order.Field.Orders: [
                    {
                        order.Field.Id: 3607,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('373.02'),
                    },
                    {
                        order.Field.Id: 3949,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('702.88'),
                    },
                ],
                order.Field.Min: Decimal('373.02'),
                order.Field.Max: Decimal('702.88'),
                order.Field.Sum: Decimal('1075.90'),
                order.Field.Avg: Decimal('537.95'),
                order.Field.Error: [],
            }],
        })
        expected = [(3, {
            customer.Field.CustomerId: 3,
            customer.Field.Detail: {
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny2bandcamp.com',
                detail.Field.Error: ['email \'wtuppeny2bandcamp.com\' is invalid'],
            },
            customer.Field.Orders: {
                order.Field.CustomerID: 3,
                order.Field.Orders: [
                    {
                        'customer_id': 3,
                        'id': 3607,
                        'total_price': Decimal('373.02'),
                    },
                    {
                        'customer_id': 3,
                        'id': 3949,
                        'total_price': Decimal('702.88'),
                    }],
                order.Field.Min: Decimal('373.02'),
                order.Field.Max: Decimal('702.88'),
                order.Field.Sum: Decimal('1075.90'),
                order.Field.Avg: Decimal('537.95'),
                order.Field.Error: [],
            },
            customer.Field.Errors: ['email \'wtuppeny2bandcamp.com\' is invalid'],
        })]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)

    def test_order_error(self):
        data = (3, {
            customer.Field.Detail: [{
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny@bandcamp.com',
            }],
            customer.Field.Orders: [{
                order.Field.CustomerID: 3,
                order.Field.Orders: [
                    {
                        order.Field.Id: 3607,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: None,
                        order.Field.Error: 'total price \'I am not a price\' is invalid',
                    },
                    {
                        order.Field.Id: 3949,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('702.88'),
                    },
                ],
                order.Field.Min: None,
                order.Field.Max: None,
                order.Field.Sum: None,
                order.Field.Avg: None,
                order.Field.Error: ['total price \'I am not a price\' is invalid'],
            }],
        })
        expected = [(3, {
            customer.Field.CustomerId: 3,
            customer.Field.Detail: {
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny@bandcamp.com',
            },
            customer.Field.Orders: {
                order.Field.CustomerID: 3,
                order.Field.Orders: [
                    {
                        order.Field.Id: 3607,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: None,
                        order.Field.Error: 'total price \'I am not a price\' is invalid',
                    },
                    {
                        order.Field.Id: 3949,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('702.88'),
                    }],
                order.Field.Min: None,
                order.Field.Max: None,
                order.Field.Sum: None,
                order.Field.Avg: None,
                order.Field.Error: ['total price \'I am not a price\' is invalid'],
            },
            customer.Field.Errors: ['total price \'I am not a price\' is invalid'],
        })]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)


class TestJsonFormatter(unittest.TestCase):

    def test_empty(self):
        data = (3, {
            order.Field.CustomerID: 3,
            customer.Field.Detail: {
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny@bandcamp.com',
            },
            customer.Field.Orders: None,
            order.Field.Min: None,
            order.Field.Max: None,
            order.Field.Sum: None,
            order.Field.Avg: None,
            customer.Field.Errors: [],
        })
        expected = ['{"errors": [], "first_name": "Winny", "id": 3, "last_name": "Tuppeny", "sum": null}']
        actual = [data] | beam.ParDo(JsonFormatter())
        self.assertEqual(expected, actual)

    def test_nonempty(self):
        data = (3, {
            order.Field.CustomerID: 3,
            customer.Field.Detail: {
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny@bandcamp.com',
            },
            customer.Field.Orders: {
                order.Field.Orders: [
                    {
                        order.Field.Id: 3607,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('373.02'),
                    },
                    {
                        order.Field.Id: 3949,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('702.88'),
                    },
                ],
                order.Field.Min: Decimal('373.02'),
                order.Field.Max: Decimal('702.88'),
                order.Field.Sum: Decimal('1075.90'),
                order.Field.Avg: Decimal('537.95'),
            },
            customer.Field.Errors: [],
        })
        expected = ['{"errors": [], "first_name": "Winny", "id": 3, "last_name": "Tuppeny", "sum": "1075.90"}']
        actual = [data] | beam.ParDo(JsonFormatter())
        self.assertEqual(expected, actual)

    def test_error(self):
        data = (3, {
            order.Field.CustomerID: 3,
            customer.Field.Detail: {
                detail.Field.Id: 3,
                detail.Field.FirstName: 'Winny',
                detail.Field.LastName: 'Tuppeny',
                detail.Field.Email: 'wtuppeny2bandcamp.com',
            },
            customer.Field.Orders: {
                order.Field.Orders: [
                    {
                        order.Field.Id: 3607,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('373.02'),
                    },
                    {
                        order.Field.Id: 3949,
                        order.Field.CustomerID: 3,
                        order.Field.TotalPrice: Decimal('702.88'),
                    },
                ],
                order.Field.Min: Decimal('373.02'),
                order.Field.Max: Decimal('702.88'),
                order.Field.Sum: Decimal('1075.90'),
                order.Field.Avg: Decimal('537.95'),
            },
            customer.Field.Errors: ['email \'wtuppeny2bandcamp.com\' is invalid'],
        })
        expected = ['{"errors": ["email \'wtuppeny2bandcamp.com\' is invalid"], "first_name": "Winny", "id": 3,' +
                    ' "last_name": "Tuppeny", "sum": "1075.90"}']
        actual = [data] | beam.ParDo(JsonFormatter())
        self.assertEqual(expected, actual)
