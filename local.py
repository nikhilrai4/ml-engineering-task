import logging
import os
from multiprocessing import Manager, Process
from typing import Any, List

import pandas as pd

from processing import process


def run_multiprocess(
    method: str,
    pool_over: str,
) -> pd.DataFrame:
    processes_used = 2
    logging.info(f"Running using multi-processing on {processes_used} processes.")

    logging.info(f"Reading data.")
    training_data_full = pd.read_json("data.json")

    logging.info(f"Pooling data by {pool_over}")
    training_data = [y for _, y in training_data_full.groupby(pool_over)]

    batches = min(processes_used, len(training_data))
    start_indices = [int(i * len(training_data) / batches) for i in range(batches)]
    end_indices = [
        len(training_data) if i == batches - 1 else start_indices[i + 1]
        for i in range(batches)
    ]
    chunks = [training_data[start_indices[i] : end_indices[i]] for i in range(batches)]
    procs: List[Process] = []
    with Manager() as manager:
        outputs = manager.list()
        for i in range(batches):
            proc = Process(
                target=estimate_batch,
                args=(
                    i + 1,
                    start_indices[i],
                    end_indices[i],
                    method,
                    chunks[i],
                    outputs,
                ),
            )
            procs.append(proc)
            proc.start()

        for proc in procs:
            proc.join()

        penetration_coefficients = pd.concat(outputs).reset_index(drop=True)

    logging.info(f"Saving output to output.json")
    penetration_coefficients.to_json("output.json")


def estimate_batch(
    batch_no: int,
    row: int,
    end_index: int,
    method: str,
    retailer_data: List[pd.DataFrame],
    outputs: Any,
) -> None:
    logging.info(f"Process {batch_no}: Handling pools {row + 1} to {end_index}")
    i = 0
    failed = 0
    t = end_index - row
    out_dfs = []
    while row < end_index:
        next_df, next_failed = process(retailer_data[i], method)
        out_dfs.append(next_df)
        failed += next_failed
        i += 1
        row += 1
    logging.info(
        f"Process {batch_no}: Handled {i} pools of {t}, with {failed} failures."
    )
    output_data = pd.concat(out_dfs)
    outputs.append(output_data)
