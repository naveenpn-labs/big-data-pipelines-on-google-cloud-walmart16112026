import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(runner="DirectRunner")


def parse_record(record):
    id,age,gen,designation,salary = record.split(",")
    return {
        "id":int(id),
        "age":int(age),
        "gen":gen,
        "designation":designation,
        "salary":int(salary)
    }

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Read Users" >> beam.io.ReadFromText(file_pattern="gs://walmart-sandbox-bucket/pyspark-jobs/dataset/users.csv",skip_header_lines=1)
        | "Parser Users" >> beam.Map(parse_record)
        | "Filter Age > 50 " >> beam.Filter(lambda user:user["age"]>50) 
        | "Print results" >> beam.Map(print)
    )