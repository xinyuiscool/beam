import unittest

import fastavro

import apache_beam as beam
from apache_beam.io import avroio
from pyarrow import fs

from apache_beam.io.filesystems import FileSystems


class TestAvro(unittest.TestCase):

    def test_avro_read(self):

        #hdfs = fs.HadoopFileSystem()

        file_name = "/Users/xiliu/ei_pve/1.avro"
        match_result = FileSystems.match([file_name], limits=[1])[0]
        if len(match_result.metadata_list) <= 0:
            raise IOError('No files found based on the file pattern %s' % file_name)
        match_path = match_result.metadata_list[0].path

        with FileSystems.open(match_path) as f:
            reader = fastavro.reader(f)
            schema = reader.writer_schema

        print(schema)


        p = beam.Pipeline()

        _ = p | avroio.ReadFromAvro(file_name)

        p.run()


if __name__ == '__main__':
    unittest.main()
