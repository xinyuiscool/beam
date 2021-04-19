import unittest
from typing import Optional, Union

import typing

import apache_beam as beam
from apache_beam.io import avroio
from apache_beam import coders
from apache_beam.dataframe import convert
import fastavro


PageView = typing.NamedTuple(
    'PageView', [('memberId', int), ('viewerUrn', Union[str, None])])
coders.registry.register_coder(PageView, coders.RowCoder)


# | beam.Map(lambda x: PageView(x['header']['memberId'], x['header']['viewerUrn']))
#   .with_output_types(PageView)
# | beam.Map(lambda x: beam.Row(memberId=x['header']['memberId'], viewerUrn=x['header']['viewerUrn']))
#   .with_output_types(typing.Tuple[int, Union[str, None]])

def toRow(x):
    r = beam.Row(**x)
    r.header = beam.Row(**r.header)
    r.requestHeader = beam.Row(**r.requestHeader)
    return r

def _get_type(ftype):
    field_type = any
    if type(ftype) == str:
        if ftype == 'int':
            field_type = int
        elif ftype == 'string':
            field_type = str
    elif type(ftype) == list:
        if len(ftype) == 2 and 'null' in ftype:
            ftype.remove('null')
            field_type = Optional[_get_type(ftype[0])]
    elif type(ftype) == dict:
        inner_type = ftype['type']
        if inner_type == 'enum':
            field_type = str
        elif inner_type == 'map':
            field_type = map
        elif inner_type == 'record':
            field_type = schema_to_namedtuple(ftype)

    return field_type

def schema_to_namedtuple(x):
    # type: (dict) -> typing.NamedTuple

    name = x['name'].rpartition('.')[-1]
    fields = []
    for field in x['fields']:
        fields.append((field['name'], _get_type(field['type'])))

    return typing.NamedTuple(name, fields)

class TestAvro(unittest.TestCase):


    def test_avro_read(self):
        p = beam.Pipeline()
        file_name = "/Users/xiliu/ei_pve/1.avro"

        with open(file_name, "rb") as f:
            reader = fastavro.reader(f)
            schema = reader.writer_schema

        schema_type = schema_to_namedtuple(schema)
        print(schema_type)

        input = (p
                | avroio.ReadFromAvro(file_name).with_output_types(schema_type)
                | beam.Map(lambda x: toRow(x)).with_output_types(schema_type))
                # | beam.Select('header.memberId', 'requestHeader.pageKey'))

        df = convert.to_dataframe(input)
        #out = df.loc[df['memberId'] == 0]

        out_pc = convert.to_pcollection(df)
        _ = out_pc | beam.Map(print)
        p.run()


if __name__ == '__main__':
    unittest.main()
