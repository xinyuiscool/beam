import unittest
import pandas as pd
import apache_beam as beam
from apache_beam.dataframe import convert


class DfTest(unittest.TestCase):

    def test_df_input(self):
        input = pd.DataFrame({
            'Animal': ['Aardvark', 'Ant', 'Elephant', 'Zebra'],
            'Speed': [5, 2, 35, 40],
            'Size': ['Small', 'Extra Small', 'Large', 'Medium']
        })
        p = beam.Pipeline()
        pc = p | beam.Create([input])
        pdf = convert.to_dataframe(pc, proxy=input.iloc[0:0])
        rdf = pdf.loc[pdf['Animal'] == 'Zebra']
        result = convert.to_pcollection(rdf)
        _ = result | beam.Map(print)

        p.run()


