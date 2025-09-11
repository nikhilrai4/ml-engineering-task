import argparse
import logging
import pprint
from enum import Enum
from typing import Final

from local import run_multiprocess
from workflow import run_beam

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class MethodOption(str, Enum):
    full = "full"
    discount = "discount"
    transactions = "transactions"
    legacy = "legacy"


method_options: Final = [o.value for o in MethodOption]


class PoolingOption(str, Enum):
    item_id = "item_id"
    lowest_category = "lowest_category"
    highest_category = "highest_category"


pooling_options: Final = [o.value for o in PoolingOption]


class ParallalizationOption(str, Enum):
    multiprocess = "multiprocess"
    beam = "beam"


parallalization_options: Final = [o.value for o in ParallalizationOption]


def run(
    method: str,
    pool_over: str,
    parallalization: str,
) -> None:
    logging.info(f"Running with parallalization method: {parallalization}")

    if parallalization == "beam":
        return run_beam(
            method,
            pool_over,
        )

    # run_multiprocess responsible for data sink.
    # If test data exists it runs a holdout test and returns penetration_coefficients, test_data, retailer_data
    # If output_table = "" then it returns penetration_coefficients
    # Otherwise it returns nothing
    return run_multiprocess(
        method,
        pool_over,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--method", choices=method_options, default="discount")
    parser.add_argument("--pool_over", choices=pooling_options, default="item_id")
    parser.add_argument(
        "--parallalization",
        choices=parallalization_options,
        default="multiprocess",
    )
    args = parser.parse_args()

    logging.info(f"Command Line Arguments:\n {pprint.pformat(vars(args))}")
    run(args.method, args.pool_over, args.parallalization)
