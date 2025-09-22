import os
import pandas as pd
import sys
import shutil
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from local import run_multiprocess
from workflow import run_beam

def test_pipelines_equivalence(tmp_path):
    # Setup: copy data.json to tmp_path if needed
    # Copy data.json from project root to tmp_path
    shutil.copyfile("data.json", tmp_path / "data.json")
    os.chdir(tmp_path)
    # Run multiprocessing pipeline
    run_multiprocess(method="discount", pool_over="item_id")
    df_mp = pd.read_json("output.json")
    # Remove output and rerun with Beam
    os.remove("output.json")
    run_beam(method="discount", pool_over="item_id")
    df_beam = pd.read_json("output.json")
    # Sort by item_id for both DataFrames
    df_mp_sorted = df_mp.sort_values(by="item_id").reset_index(drop=True)
    df_beam_sorted = df_beam.sort_values(by="item_id").reset_index(drop=True)
    # Compare DataFrames
    pd.testing.assert_frame_equal(df_mp_sorted.sort_index(axis=1), df_beam_sorted.sort_index(axis=1))

