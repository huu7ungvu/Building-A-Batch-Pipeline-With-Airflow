import apache_beam as beam
from apache_beam.io import ReadFromAvro, WriteToBigQuery, WriteToText
from apache_beam.transforms import PTransform, GroupByKey, Map
from decimal import Decimal

class ReadAvroFromGCS(PTransform):
    def __init__(self, avro_file_pattern):
        self.avro_file_pattern = avro_file_pattern

    def expand(self, pcollection):
        return (
            pcollection
            | 'ReadAvro' >> ReadFromAvro(self.avro_file_pattern)
        )

class RemoveDuplicates(PTransform):
    def __init__(self, unique_key):
        self.unique_key = unique_key

    def expand(self, pcollection):
        return (
            pcollection
            | 'PairWithKey' >> Map(lambda x: (x[self.unique_key], x))
            | 'GroupByKey' >> GroupByKey()
            | 'RemoveDuplicates' >> beam.FlatMap(self._remove_duplicates)
        )

    def _remove_duplicates(self, element):
        import time
        from datetime import datetime

        key, grouped_elements = element
        latest_record = max(grouped_elements, key = lambda elem: int(time.mktime(datetime.strptime(elem['last_updated_at'], "%Y-%m-%d %H:%M:%S").timetuple()))) 
        yield latest_record

class PrintElementType(beam.DoFn):
    def process(self, element):
        # Print the type of each element and its values
        for key, value in element.items():
            print(f'Key: {key}, Value: {value}, Type: {type(value)}')
        # print(element)
        yield element

class ConvertFloatToDecimal(beam.DoFn):
    def __init__(self, convert_features):
        self.convert_features = convert_features

    def process(self,element):
        for i in self.convert_features:
            element[i] = round(Decimal(element[i]),2)
        yield element

class MaxDateFn(beam.CombineFn):
    def create_accumulator(self):
        return ''
    
    def add_input(self, mutable_accumulator, element):
        return max(mutable_accumulator, element)

    def merge_accumulators(self, accumulators):
        return max(accumulators)

    def extract_output(self, accumulator):
        return accumulator

class PushToBigQuery(PTransform):
    def __init__(self, table_spec, schema):
        self.table_spec = table_spec
        self.schema = schema

    def expand(self, pcollection):
        return (
            pcollection
            | 'WriteToBigQuery' >> WriteToBigQuery(
                self.table_spec,
                schema=self.schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
class WriteLogGCS(PTransform):
    def __init__(self, path):
        self.path = path
    
    def expand(self, pcollection):
        return (
            pcollection
            | 'WriteLogGCS' >> WriteToText(self.path,file_name_suffix=".txt")
        )