# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import json

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.options.value_provider import ValueProvider  # pylint: disable=unused-import

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

    def __init__(self, aggregation):
        # type: (ValueProvider) -> None
        super(JsonFormatter, self).__init__()
        self._aggregation = aggregation

    def process(self, element, *args, **kwargs):
        customer_id, entry = element
        customer_detail = entry.get(Field.Detail)
        customer_orders = entry.get(Field.Orders)
        if not customer_detail:
            customer_detail = {}
        if not customer_orders:
            customer_orders = {}
        output = {
            'id': customer_id,
            'first_name': customer_detail.get(detail.Field.FirstName),
            'last_name': customer_detail.get(detail.Field.LastName),
            # self.aggregation is a ValueProvider. We need to `get` the value.
            self._aggregation.get(): customer_orders.get(self._aggregation.get()),
            'errors': entry.get(Field.Errors),
        }
        yield json.dumps(output, sort_keys=True, cls=common.PipelineJSONEncoder)


class CustomerPipelineOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
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
        parser.add_value_provider_argument(
            '--output_aggregation',
            type=str,
            help='Aggregation that is output',
            choices=['min', 'max', 'sum', 'avg']
        )


def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    # Save the main session that defines global import, functions and variables. Otherwise they are not saved during
    # the serialization.
    # Details see https://cloud.google.com/dataflow/docs/resources/faq#how_do_i_handle_nameerrors
    pipeline_options.view_as(SetupOptions).save_main_session = True
    # Get our custom options
    customer_options = pipeline_options.view_as(CustomerPipelineOptions)
    with beam.Pipeline(options=pipeline_options) as p:
        # pylint: disable=expression-not-assigned
        detail_valid, detail_broken = (p | 'detail' >> detail.Prepare(customer_options.detail_input))
        order_valid, order_broken = (p | 'order' >> order.Prepare(customer_options.order_input))

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
            | 'format_json' >> beam.ParDo(JsonFormatter(customer_options.output_aggregation))
        )

        formatted | 'write_file' >> WriteToText(customer_options.output, '.json')
