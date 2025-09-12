# workflow.py

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, DirectOptions

def run_beam():
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
            | "Log" >> beam.Map(print)    # Debug step
        )
