import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(runner="DirectRunner")

with beam.Pipeline() as p:
    numbers = (
        p
        | "Create numbers" >> beam.Create([10,20,30])
        | "Print Numbers" >> beam.Map(print)
    )