# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import decimal
from decimal import Decimal
import json

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.metrics import Metrics
import six


# pylint: disable=too-few-public-methods
class Field(object):

    Id = 'id'
    CustomerID = 'customer_id'
    TotalPrice = 'total_price'

    Element = 'element'
    Error = 'error'


class Parse(beam.DoFn):
    """Parses a JSON string to an dict."""

    TAG_BROKEN_DATA = 'broken_data'

    def __init__(self):
        super(Parse, self).__init__()
        self.broken_data_counter = Metrics.counter(self.__class__, 'errors')

    def process(self, element, *args, **kwargs):
        try:
            row = json.loads(element, encoding='utf-8')
            yield self.parse_row(row)
        except (TypeError, ValueError) as e:
            yield pvalue.TaggedOutput(self.TAG_BROKEN_DATA, {Field.Element: element, Field.Error: e.message})
            self.broken_data_counter.inc()

    @staticmethod
    def parse_row(row):
        order_id = row.get('id')
        if not order_id:
            raise ValueError('id is missing')
        return order_id, {
            Field.Id: order_id,
            Field.CustomerID: row.get('customer_id'),
            Field.TotalPrice: row.get('total_price'),
        }


class Validate(beam.DoFn):

    def process(self, element, *args, **kwargs):
        order_id, entry = element
        errors = []
        customer_id = entry.get(Field.CustomerID)
        if not customer_id:
            errors.append('customer id is missing')

        total_price_raw = entry.get(Field.TotalPrice)
        total_price = None
        if not total_price_raw:
            errors.append('total price is missing')
        elif not isinstance(total_price_raw, six.string_types):
            errors.append('total price \'{}\' is invalid'.format(total_price_raw))
        else:
            try:
                total_price = Decimal(total_price_raw)
            except (TypeError, decimal.InvalidOperation):
                errors.append('total price \'{}\' is invalid'.format(total_price_raw))

        yield order_id, {
            Field.Id: order_id,
            Field.CustomerID: customer_id,
            Field.TotalPrice: total_price,
            Field.Error: errors
        }


class Prepare(beam.PTransform):

    def __init__(self, file_pattern):
        super(Prepare, self).__init__('order')
        self.file_pattern = file_pattern

    def expand(self, input_or_inputs):

        parsed_records = (
            input_or_inputs
            | 'read' >> ReadFromText(self.file_pattern)
            | 'parse' >> beam.ParDo(Parse()).with_outputs(Parse.TAG_BROKEN_DATA, main='parsed')
        )

        broken_records = parsed_records[Parse.TAG_BROKEN_DATA]

        valid_records = (
            parsed_records['parsed']
            | 'validate' >> beam.ParDo(Validate())
        )

        return valid_records, broken_records
