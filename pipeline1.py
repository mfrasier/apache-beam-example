import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import time

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
logger = logging.getLogger("pipeline1")


class AddTimestampDoFn(beam.DoFn):
    """ Add timestamp to PCollection. """

    def process(self, element):
        # extract event time from element.registration_dttm field
        event_time = extract_event_time(element)
        # wrap and emit current element and new timestamp an a TimestampedValue
        yield beam.window.TimestampedValue(element, event_time)


def extract_event_time(element):
    """ Extract event time from element. """
    time_tuple = element['registration_dttm'].to_pydatetime().timetuple()
    unix_time = time.mktime(time_tuple)
    return unix_time


def run(pipeline_options, file_pattern):
    logger.info("retrieving data from " + file_pattern)
    with beam.Pipeline(options=pipeline_options) as p:
        user_data = (
                p
                | 'read' >> beam.io.ReadFromParquet(file_pattern)
                | 'timestamp' >> beam.ParDo(AddTimestampDoFn())
                | 'window' >> beam.WindowInto(beam.window.FixedWindows(60 * 60))
                # | 'count elements' >> beam.combiners.Count.PerKey()
                | 'print' >> beam.Map(print))


if __name__ == "__main__":
    parquet_files = "data/userdata*.parquet"
    pipeline_options = PipelineOptions(runner='direct')
    run(pipeline_options, parquet_files)
