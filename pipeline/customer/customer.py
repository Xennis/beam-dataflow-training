# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import json

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from pipeline.common import common
from pipeline.customer import detail, order


# pylint: disable=too-few-public-methods
class Field(object):
    CustomerId = 'customer_id'
    Detail = 'detail'
    Orders = 'orders'

    Errors = 'errors'


class Validate(beam.DoFn):

    def process(self, element, *args, **kwargs):
        customer_id, entry = element

        # we only get iterables but we need lists to check the length
        details = list(entry.get(Field.Detail, []))
        orders = list(entry.get(Field.Orders, []))
        errors = []

        customer_detail = None
        if not details:
            errors.append('no customer details')
        elif len(details) == 1:
            customer_detail = details[0]
            derr = customer_detail.get(detail.Field.Error)
            if derr:
                errors.extend(derr)
        else:
            errors.append('multiple customer details')

        customer_orders = None
        if len(orders) > 1:
            # Alternatively, skip the GroupByKey before, have it been grouped here, and do the aggregation afterwards.
            errors.append('multiple order records')
        elif orders:
            customer_orders = orders[0]
            errors.extend(customer_orders.get(order.Field.Error))

        yield customer_id, {
            Field.CustomerId: customer_id,
            Field.Detail: customer_detail,
            Field.Orders: customer_orders,
            Field.Errors: errors,
        }


class JsonFormatter(beam.DoFn):

    def process(self, element, *args, **kwargs):
        customer_id, entry = element
        customer_detail = entry.get(Field.Detail)
        customer_orders = entry.get(Field.Orders)
        if not customer_detail:
            customer_detail = {}
        if not customer_orders:
            customer_orders = {}
        yield json.dumps({
            'id': customer_id,
            'first_name': customer_detail.get(detail.Field.FirstName),
            'last_name': customer_detail.get(detail.Field.LastName),
            'sum': customer_orders.get(order.Field.Sum),
            'errors': entry.get(Field.Errors),
        }, sort_keys=True, cls=common.PipelineJSONEncoder)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--detail_input',
        type=str,
        help='Input file pattern for the customer details',
        required=True)
    parser.add_argument(
        '--order_input',
        type=str,
        help='Input file pattern for the customer orders',
        required=True)
    parser.add_argument(
        '--output',
        type=str,
        help='Output file pattern',
        required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    # Save the main session that defines global import, functions and variables. Otherwise they are not saved during
    # the serialization.
    # Details see https://cloud.google.com/dataflow/docs/resources/faq#how_do_i_handle_nameerrors
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:
        # pylint: disable=expression-not-assigned
        detail_valid, detail_broken = (p | 'detail' >> detail.Prepare(known_args.detail_input))
        order_valid, order_broken = (p | 'order' >> order.Prepare(known_args.order_input))

        detail_broken | 'broken_details' >> common.Log(prefix="Broken Details")
        order_broken | 'broken_orders' >> common.Log(prefix="Broken Orders")

        aggregated_orders = (
            order_valid
            | 'orders_by_customer' >> order.GroupByCustomer()
            | 'aggregate_orders' >> beam.ParDo(order.AggregateOrders())
        )

        formatted = (
            {
                Field.Detail: detail_valid,
                Field.Orders: aggregated_orders,
            }
            | 'join' >> beam.CoGroupByKey()
            | 'validate' >> beam.ParDo(Validate())
            | 'format_json' >> beam.ParDo(JsonFormatter())
        )

        formatted | 'write_file' >> WriteToText(known_args.output, '.json')
