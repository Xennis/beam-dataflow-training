# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import json

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.metrics import Metrics


# pylint: disable=too-few-public-methods
class Field(object):

    Id = 'id'
    FirstName = 'first_name'
    LastName = 'last_name'
    Email = 'email'

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
        detail_id = row.get('id')
        if not detail_id:
            raise ValueError('id is missing')
        return detail_id, {
            Field.Id: detail_id,
            Field.FirstName: row.get('first_name'),
            Field.LastName: row.get('last_name'),
            Field.Email: row.get('email')
        }


class Validate(beam.DoFn):

    def process(self, element, *args, **kwargs):
        detail_id, entry = element
        errors = []
        first_name = entry.get(Field.FirstName)
        if not first_name:
            errors.append('first name is missing')
        last_name = entry.get(Field.LastName)
        if not last_name:
            errors.append('last name is missing')
        email = entry.get(Field.Email)
        if email and '@' not in email:  # Email is optional
            errors.append('email \'{}\' is invalid'.format(email))
            email = None

        yield detail_id, {
            Field.Id: detail_id,
            Field.FirstName: first_name,
            Field.LastName: last_name,
            Field.Email: email,
            Field.Error: errors
        }


class Prepare(beam.PTransform):

    def __init__(self, file_pattern):
        super(Prepare, self).__init__('details')
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
