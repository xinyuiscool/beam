import unittest
from typing import Optional, Union

import typing

import apache_beam as beam
from apache_beam.io import avroio
from apache_beam import coders
from apache_beam.dataframe import convert
from apache_beam.dataframe.io import read_csv


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

class TestAvro(unittest.TestCase):

    def test_csv_read(self):
        p = beam.Pipeline()
        df = p | read_csv("/Users/xiliu/ei_pve/1.csv")
        output = convert.to_pcollection(df)
        _ = output | beam.Map(print)
        p.run()


if __name__ == '__main__':
    unittest.main()
