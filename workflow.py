"""
TARGET: beam equivalent of your multiprocessing pipeline:
  1) read json file (array, dict-of-lists, dict-of-dicts, or ndjson)
  2) flatten to rows (handles dict-of-dicts like your sample)
  3) normalize keys (fix dict item_id / group key -> scalar)
  4) groupby(pool_over) → process(df, method)
  5) concat outputs → sort naturally by item_id, reset index
  6) write single-line `output.json` (pandas to_json format)
runs in parallel on directrunner using the chosen running mode and worker count.
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, DirectOptions
from apache_beam.io import fileio

def run_beam(method: str,
    pool_over: str):
    # Configure pipeline options
    options = PipelineOptions(
        runner="DirectRunner",       # Local execution
        save_main_session=True       # Ensures global imports are available on workers
    )

    # DirectRunner-specific options
    direct_opts = options.view_as(DirectOptions)
    direct_opts.direct_running_mode = "multi_threading"  # or "multi_processing"
    direct_opts.direct_num_workers = 4

    # Create and run the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Start" >> beam.Create([])  # Placeholder for now
            | "Match Input" >> fileio.MatchFiles("data.json")
            | "Read files" >> fileio.ReadMatches()           
            | "Read contents" >> beam.Map(lambda file: file.read_utf8())
            | "Log" >> beam.Map(print)    # Debug step
        )
