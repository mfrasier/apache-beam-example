import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
logger = logging.getLogger("pipeline1")


def run(pipeline_options, file_pattern):
    logger.info("retrieving data from " + file_pattern)
    with beam.Pipeline(options=pipeline_options) as p:
        user_data = (
                p
                | beam.io.ReadFromParquet(file_pattern))
        logger.info("retrieved data")
        # user_data |
        # print(f"retrieved {user_data.count()} records from {file_pattern}")


if __name__ == "__main__":
    parquet_files = "data/userdata*.parquet"
    pipeline_options = PipelineOptions(runner='direct')
    run(pipeline_options, parquet_files)
