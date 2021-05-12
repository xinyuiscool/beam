import unittest
from typing import Optional, Union

import typing

from pandas import DataFrame

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import avroio
from apache_beam.dataframe import convert
import fastavro
import numpy as np

from apache_beam.transforms.combiners import Count, CountCombineFn


def to_row(x):
    # type: (dict) -> beam.Row

    """Converts a dict object to beam Row

    This method converts the {key:value} in a dict
    to the fields in Row object to allow schema transforms.
    """
    return beam.Row(**{
        key: to_row(value) if type(value) == dict else value
        for key, value in x.items()
    })


def schema_to_type(schema):
    # type: (any) -> any

    """Converts avro schema to python typing

    This method converts from avro schema to the python type.
    For avro record, it will convert it to typing.NamedTuple.
    """
    field_type = any

    if schema == 'int':
        field_type = int
    elif schema == 'string':
        field_type = str
    elif type(schema) == dict:
        inner_type = schema['type']
        if inner_type == 'record':
            fields = []
            for field in schema['fields']:
                fields.append((field['name'], schema_to_type(field['type'])))
            field_name = schema['name'].rpartition('.')[-1]
            field_type = typing.NamedTuple(field_name, fields)
        elif inner_type == 'enum':
            field_type = str
        elif inner_type == 'map':
            field_type = map
    elif type(schema) == list:
        if len(schema) == 2 and 'null' in schema:
            schema.remove('null')
            field_type = Optional[schema_to_type(schema[0])]

    return field_type

def test(x):
    return x

Page = typing.NamedTuple("Page", [('memberId', int), ('pageKey', str)])
coders.registry.register_coder(Page, coders.RowCoder)

class TestAvro(unittest.TestCase):

    def test_avro_read(self):
        p = beam.Pipeline()
        file_name = "/Users/xiliu/ei_pve/1.avro"

        with open(file_name, "rb") as f:
            reader = fastavro.reader(f)
            schema = reader.writer_schema

        schema_type = schema_to_type(schema)

        input = (p
                 | avroio.ReadFromAvro(file_name).with_output_types(schema_type)
                 | beam.Map(to_row).with_output_types(schema_type)
                 | beam.Select('header.memberId', 'requestHeader.pageKey'))
                 #| beam.Filter(lambda x: x.memberId == 0)
                 #| "unique" >> beam.GroupBy('pageKey', 'memberId').aggregate_field('memberId', lambda x: 1, "count")
                 #| "count" >> beam.GroupBy('pageKey').aggregate_field("memberId", CountCombineFn(), "pageCount")
                 #| beam.Map(print))

        df: DataFrame = convert.to_dataframe(input)

        data = DataFrame.from_dict({'pageKey': ["realtime-frontend-connect", 'ios', 'home', 'index'], 'job': ['engineer', 'doctor', 'driver', 'cook']})
        input2 = p | beam.Create([data])
        df2: DataFrame = convert.to_dataframe(input2, proxy=data.iloc[0:0])

        #output_df = df['memberId'] * 10
        #df[] = df.agg({'memberId': ['sum', 'min']})
        # df = df.query('memberId == 0 and pageKey == "realtime-frontend-connect"')


        #df = df.set_index('memberId').join(df2.set_index('memberId'))
        df = df.agg({'memberId': ['count']})

        #df = df.cummax()
        # df = df.groupby('memberId').count()

        #filtered_df = df['memberId' == 0]
        #filtered_df = df.loc[df['memberId'] == 0]


        out_pc = convert.to_pcollection(df)
        _ = out_pc | beam.Map(print)
        p.run()


if __name__ == '__main__':
    unittest.main()
