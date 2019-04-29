import json
from copy import deepcopy
from pyspark.sql.types import *


class Extractor(object):
    conf = {}
    types_dict = {
        "string": StringType(),
        "float": FloatType(),
        "int": IntegerType(),
        "date": DateType()
    }

    def __init(self,conf_file):
        def _get_configuration(conf_file):
            with open(self.configuration_file) as f:
                return json.load(f)
        self.conf = _get_configuration(self)

    def extract_csv(self, sc, sqlCtx):

        def _get_csv_record(line, delimiter):
            return line.split(delimiter)

        def _line_to_tuple(line):
            return tuple(line)

        def csv_to_rdd(file_name,delimiter):
            return sc.textFile(file_name)\
                .map(lambda line: _get_csv_record(line, delimiter))\
                .filter(lambda line: len(line)>1) \
                .map(lambda line: _line_to_tuple)

        def rdd_to_dataframe(self, rdd, sqlCtx, conf):
            fields = [StructField(field['name'],
                                  self.types_dict[field['type']], True)
                      for field in conf['metadata']['file_schema']]
            schema = StructType(fields)
            return sqlCtx.createDataFrame(rdd, schema)

        _conf = deepcopy(self.conf)
        directory_path = _conf['metadata']['directory_path']
        file_name = '/'.join([directory_path, _conf['metadata']['file_name']])
        delimiter = _conf['metadata']['delimiter']
        rdd = csv_to_rdd(file_name, delimiter)
        df = rdd_to_dataframe(self, rdd, sqlCtx, _conf)
        return df

    def extract_parquet(self,sqlCtx):
        _conf = deepcopy(self.conf)
        input_directory = _conf['metadata']['input_directory']
        df = sqlCtx.read.parquet(input_directory)
        return df


