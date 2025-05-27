import apache_beam as beam
import csv
from io import StringIO
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions, WorkerOptions



# Custom options including input and output and all pipeline args
class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls,parser):
        parser.add_argument('--input', required=True, help='GCS path as the input file')
        parser.add_argument('--output', required=True, help='Bigquery table in format project:dataset.table')

 
# DoFn to parse and transform CSV lines
class ParseAndTransformRecord(beam.DoFn):
    def process(self, element):
        # Use csv reader to handle quoted addresses properly
        reader = csv.reader(StringIO(element))
        row = next(reader)

        if len(row) != 8:
            return # skip bad rows
        emp_id, name, age, dept, designation, salary, phone, address = row

        # split rows
        try:
            street, city, state = [x.strip() for x in address.split(',')]
        except ValueError:
            street, city, state = address, "",""

        yield {
            'id': int(emp_id),
            'name': name,
            'age': int(age),
            'department': dept,
            'designation': designation,
            'salary': float(salary),
            'phone': phone,
            'street': street,
            'city': city,
            'state': state
        }


bq_schema = "id:INTEGER,name:STRING,age:INTEGER,department:STRING,designation:STRING,salary:FLOAT,phone:STRING,street:STRING,city:STRING,state:STRING"

def run():
    options = MyOptions()
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(WorkerOptions).num_workers = 2
    options.view_as(WorkerOptions).max_num_workers = 20
    options.view_as(WorkerOptions).machine_type = 'n1-standard-1'
    # To enable saving the main session (needed for some remote executions)
    options.view_as(SetupOptions).save_main_session = True

    input_path = options.input
    output_table = options.output


    with beam.Pipeline(options = options) as p:
        data = ( 
        p | 'reading data' >> beam.io.ReadFromText(input_path, skip_header_lines=1)
          | 'pardo calling' >> beam.ParDo(ParseAndTransformRecord())
          | 'write into bq' >> beam.io.WriteToBigQuery(
              table = output_table,
              schema = bq_schema,
              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
          )
    )
if __name__ == '__main__':
    run()