# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os

import unittest

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipeline.customer.detail import Parse, Validate, Prepare


class TestParse(unittest.TestCase):

    def test_valid(self):
        data = '{"id":1,"first_name":"Bart","last_name":"Bruck","email":"bbruck0@comsenz.com"}'
        parsed_expected = [
            (1, {
                'email': 'bbruck0@comsenz.com',
                'first_name': 'Bart',
                'id': 1,
                'last_name': 'Bruck'
            })
        ]
        broken_expected = []

        actual = [data] | beam.ParDo(Parse()).with_outputs(Parse.TAG_BROKEN_DATA, main='parsed')
        self.assertEqual(parsed_expected, actual['parsed'])
        self.assertEqual(broken_expected, actual[Parse.TAG_BROKEN_DATA])

    def test_id_only(self):
        data = '{"id":1}'
        parsed_expected = [
            (1, {
                'email': None,
                'first_name': None,
                'id': 1,
                'last_name': None
            })
        ]
        broken_expected = []

        actual = [data] | beam.ParDo(Parse()).with_outputs(Parse.TAG_BROKEN_DATA, main='parsed')
        self.assertEqual(parsed_expected, actual['parsed'])
        self.assertEqual(broken_expected, actual[Parse.TAG_BROKEN_DATA])

    def test_missing_id(self):
        data = '{"first_name":"Bart","last_name":"Bruck","email":"bbruck0@comsenz.com"}'
        parsed_expected = []
        broken_expected = [
            {
                'element': '{"first_name":"Bart","last_name":"Bruck","email":"bbruck0@comsenz.com"}',
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
        data = (1, {
            'email': 'bbruck0@comsenz.com',
            'first_name': 'Bart',
            'id': 1,
            'last_name': 'Bruck'
        })
        expected = [
            (1, {
                'email': 'bbruck0@comsenz.com',
                'error': [],
                'first_name': 'Bart',
                'id': 1,
                'last_name': 'Bruck'
            })
        ]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)

    def test_id_only(self):
        data = (1, {
            'email': None,
            'first_name': None,
            'id': 1,
            'last_name': None
        })
        expected = [
            (1, {
                'email': None,
                'error': ['first name is missing', 'last name is missing'],
                'first_name': None,
                'id': 1,
                'last_name': None
            })
        ]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)

    def test_invalid_mail(self):
        data = (1, {
            'email': 'I am not a mail',
            'first_name': 'Bart',
            'id': 1,
            'last_name': 'Bruck'
        })
        expected = [
            (1, {
                'email': None,
                'error': ['email \'I am not a mail\' is invalid'],
                'first_name': 'Bart',
                'id': 1,
                'last_name': 'Bruck'
            })
        ]
        actual = [data] | beam.ParDo(Validate())
        self.assertEqual(expected, actual)


class TestPrepare(unittest.TestCase):

    test_data_dir = FileSystems.join(os.path.dirname(os.path.realpath(__file__)), 'testdata')

    def test_valid(self):
        file_pattern = FileSystems.join(self.test_data_dir, 'detail.json')
        expected_valid = [
            (1, {
                'error': [],
                'first_name': 'Bart',
                'last_name': 'Bruck',
                'email': 'bbruck0@comsenz.com',
                'id': 1
            }),
            (3, {
                'error': [u"email 'wtuppeny2bandcamp.com' is invalid"],
                'first_name': 'Winny',
                'last_name': 'Tuppeny',
                'email': None,
                'id': 3
            })
        ]
        expected_broken = [
            {
                'error': 'id is missing',
                'element': '{"first_name":"Alfonso","last_name":"Koenen","email":"akoenen1@admin.ch"}'
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
