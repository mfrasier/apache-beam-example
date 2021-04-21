from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import time
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
logger = logging.getLogger("pipeline1")


class SumAccumulatorFn(beam.CombineFn):
    def create_accumulator(self, *args, **kwargs):
        time = datetime.Now()
        sum = 0
        print(f"kwargs: {kwargs}")
        accumulator = time, sum
        return accumulator

    def add_input(self, mutable_accumulator, element, *args, **kwargs):
        print(f"element: {element}")
        window, sum = mutable_accumulator
        return window, sum + 1

    def merge_accumulators(self, accumulators, *args, **kwargs):
        print(f"accumulators: {accumulators}")
        windows, counts = zip(*accumulators)
        return windows, sum(counts)

    def extract_output(self, accumulator, *args, **kwargs):
        print(f"accumulator: {accumulator}")
        window, sum = accumulator
        return window, sum


class AddTimestampDoFn(beam.DoFn):
    """ Add timestamp to PCollection. """

    def process(self, element):
        # extract event time from element.registration_dttm field
        event_time = extract_event_time(element)
        # wrap and emit current element and new timestamp an a TimestampedValue
        yield beam.window.TimestampedValue(element, event_time)


class FormatCountryDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        ts_format = '%Y-%m-%d %H:%M UTC'
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        return [{
            'country': element[0],
            'count': element[1],
            'window_start': window_start,
            'window_end': window_end
        }]


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
                | 'set timestamp' >> beam.ParDo(AddTimestampDoFn())
                | 'pair with one' >> beam.Map(lambda x: (x, 1))
                | 'window' >> beam.WindowInto(beam.window.FixedWindows(60 * 60))
                # | 'combine' >> beam.CombinePerKey(SumAccumulatorFn())
                # | 'count elements' >> beam.combiners.Count.Globally().without_defaults()
                | 'print' >> beam.Map(print))


def country_counts_global(pipeline_options, file_pattern):
    logger.info("counting users by country from " + file_pattern)
    with beam.Pipeline(options=pipeline_options) as p:
        def count_countries(country_ones):
            (country, ones) = country_ones
            return country, sum(ones)

        count_by_country = (
            p
            | 'read' >> beam.io.ReadFromParquet(file_pattern)
            # | 'set timestamp' >> beam.ParDo(AddTimestampDoFn())
            # | 'window' >> beam.WindowInto(beam.window.FixedWindows(60 * 60, 0))
            | 'pair country with one' >> beam.Map(lambda x: (x['country'], 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(count_countries))
            # | 'format' >> beam.ParDo(FormatCountryDoFn()))

        count_by_country | 'print' >> beam.Map(print)


def country_counts_windowed(pipeline_options, file_pattern):
    logger.info("counting users by country from " + file_pattern)
    with beam.Pipeline(options=pipeline_options) as p:
        def count_countries(country_ones):
            (country, ones) = country_ones
            return country, sum(ones)

        count_by_country = (
            p
            | 'read' >> beam.io.ReadFromParquet(file_pattern)
            | 'set timestamp' >> beam.ParDo(AddTimestampDoFn())
            | 'window' >> beam.WindowInto(beam.window.FixedWindows(60 * 30, 0))
            | 'pair country with one' >> beam.Map(lambda x: (x['country'], 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(count_countries)
            | 'format' >> beam.ParDo(FormatCountryDoFn()))

        # count_by_country | 'print' >> beam.Map(print)
        count_by_country | beam.io.WriteToText(file_path_prefix="country_counts_windowed", file_name_suffix=".txt", num_shards=1)
        logger.info(f"writing results to country_counts_windowed*.txt")


if __name__ == "__main__":
    parquet_files = "data/userdata*.parquet"
    pipeline_options = PipelineOptions(runner='direct')
    # run(pipeline_options, parquet_files)
    # country_counts_global(pipeline_options, parquet_files)

    pipeline_options = PipelineOptions(runner='direct', job_endpoint='localhost:8099', environment_type='LOOPBACK')
    country_counts_windowed(pipeline_options, parquet_files)
