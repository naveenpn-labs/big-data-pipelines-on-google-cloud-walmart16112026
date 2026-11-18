import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(runner="DirectRunner")

with beam.Pipeline() as p:
    numbers = (
        p
        | "Create numbers" >> beam.Create(list(range(1,101)))
        | "Multiply each element by 3" >> beam.Map(lambda e:e*3)
        | "Filter numbers divisible by 15" >> beam.Filter(lambda e:e%15==0)
        | "Print numbers" >> beam.Map(print)
    )