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
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, DirectOptions
from apache_beam.io import fileio
from typing import Any, Dict, Iterable, List, Tuple
from processing import process
import pandas as pd

# ------------------------------- JSON reading -------------------------------- #

def _is_dict_of_dicts(obj: Dict[str, Any]) -> bool:
    """Returns True if all top-level values are dicts (column -> {index: value})."""
    if not isinstance(obj, dict) or not obj:
        return False
    return all(isinstance(v, dict) for v in obj.values())


def _rows_from_dict_of_dicts(obj: Dict[str, Dict[str, Any]]) -> Iterable[Dict]:
    """
    flatten pandas 'orient=columns' JSON:
      { col: { index: value }, ... }  -->  rows: [{col: value, ...}, ...]
    * uses the union of indices across columns (missing -> None)
    * sorts indices numerically if they look like integers otherwise alphabetically
    """
    # union of all index keys across columns
    all_idx_keys = set()
    for col_dict in obj.values():
        all_idx_keys.update(col_dict.keys())

    def _int_like(s: str) -> bool:
        try:
            int(s)
            return True
        except Exception:
            return False

    # sort indices
    if all(_int_like(k) for k in all_idx_keys):
        ordered_keys = sorted(all_idx_keys, key=lambda k: int(k))
    else:
        ordered_keys = sorted(all_idx_keys)

    # emit rows
    for idx in ordered_keys:
        row = {col: col_dict.get(idx) for col, col_dict in obj.items()}
        yield row


def _rows_from_json_obj(obj: Any) -> Iterable[Dict]:
    """
    convert loaded JSON into row dicts:
      - list[dict] -> rows
      - dict[str, list] -> column-oriented lists (expand)
      - dict[str, dict] -> column-oriented dicts (expand)
      - dict[str, scalar] -> single row
    """
    if isinstance(obj, list):
        for item in obj:
            if not isinstance(item, dict):
                raise ValueError("JSON array must contain dict objects.")
            yield item

    elif isinstance(obj, dict):
        values = list(obj.values())
        if values and all(isinstance(v, list) for v in values):
            # dict-of-lists
            lengths = {len(v) for v in values}
            if len(lengths) != 1:
                raise ValueError("Column-oriented JSON has inconsistent list lengths.")
            n = next(iter(lengths))
            keys = list(obj.keys())
            for i in range(n):
                yield {k: obj[k][i] for k in keys}
        elif _is_dict_of_dicts(obj):
            # dict-of-dicts (pandas default to_json orient='columns')
            yield from _rows_from_dict_of_dicts(obj)
        else:
            # Single record
            yield obj

    else:
        raise ValueError("Unsupported top-level JSON type. Must be object or array.")
    

def _rows_from_json_obj(obj: Any) -> Iterable[Dict]:
    """
    Convert loaded JSON into row dicts:
      - list[dict]           -> rows
      - dict[str, list]      -> column-oriented lists (expand)
      - dict[str, dict]      -> column-oriented dicts (expand)  <-- your case
      - dict[str, scalar]    -> single row
    """
    if isinstance(obj, list):
        for item in obj:
            if not isinstance(item, dict):
                raise ValueError("JSON array must contain dict objects.")
            yield item

    elif isinstance(obj, dict):
        values = list(obj.values())
        if values and all(isinstance(v, list) for v in values):
            # dict-of-lists
            lengths = {len(v) for v in values}
            if len(lengths) != 1:
                raise ValueError("Column-oriented JSON has inconsistent list lengths.")
            n = next(iter(lengths))
            keys = list(obj.keys())
            for i in range(n):
                yield {k: obj[k][i] for k in keys}
        elif _is_dict_of_dicts(obj):
            # dict-of-dicts (pandas default to_json orient='columns')
            yield from _rows_from_dict_of_dicts(obj)
        else:
            # Single record
            yield obj

    else:
        raise ValueError("Unsupported top-level JSON type. Must be object or array.")
    
class ReadJsonFlexibleAsRows(beam.DoFn):
    """
    Reads full file content and yields row dicts.
    - First tries as a single JSON document (array/object).
    - If that fails, falls back to NDJSON (one JSON object per line).
    """
    def process(self, file_content: str) -> Iterable[Dict]:
        txt = file_content.strip()
        if not txt:
            return
        try:
            obj = json.loads(txt)
            yield from _rows_from_json_obj(obj)
            return
        except json.JSONDecodeError:
            # Fallback: NDJSON
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)

# ------------------------------ normalization -------------------------------- #

_SCALAR_TYPES = (str, int, float, bool, type(None))

def _to_scalar(value: Any) -> Any:
    """
    make value hashable/pandas-friendly:
      - If scalar -> keep
      - If dict/list -> deterministic JSON string
    """
    if isinstance(value, _SCALAR_TYPES):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _normalize_item_id(value: Any) -> Any:
    """
    Ensure "pool_over" is a scalar (preferred id-like key if present).
    """
    if isinstance(value, _SCALAR_TYPES):
        return value
    if isinstance(value, dict):
        for k in ("id", "item_id", "sku", "code"):
            v = value.get(k)
            if isinstance(v, _SCALAR_TYPES):
                return v
    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], _SCALAR_TYPES):
        return value[0]
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


class NormalizeForProcessing(beam.DoFn):
    """
    normalize critical fields before grouping/processing:
      - `pool_over` (group key used by GroupByKey)
      - `item_id` (required by processing.process for .unique())
    """
    def __init__(self, pool_over: str):
        self.pool_over = pool_over

    def process(self, row: Dict) -> Iterable[Dict]:
        if self.pool_over not in row:
            raise KeyError(f"Column '{self.pool_over}' not found in input rows.")

        row[self.pool_over] = _to_scalar(row[self.pool_over])
        if "item_id" in row:
            row["item_id"] = _normalize_item_id(row["item_id"])
        yield row

# -------------------------- group & call process() ------------------------- #

_NUMERIC_COLS = [
    "promo_quantity",
    "sales_quantity",
    "effective_discount",
    "theoretical_discount",
]

class ProcessGroup(beam.DoFn):
    """
    For each (key, rows):
      - Build DataFrame
      - Coerce numeric columns used by your logic
      - Call processing.process(df, method) -> (out_df, failed)
      - Emit rows of out_df as dicts (records)
    """
    def __init__(self, method: str):
        self.method = method

    def process(self, kv: Tuple[Any, Iterable[Dict]]) -> Iterable[Dict]:
        group_key, rows_iter = kv
        rows = list(rows_iter)
        if not rows:
            return

        df = pd.DataFrame(rows)

        # Call processing.process function
        out_df, _failed = process(df, self.method)

        # Emit as records
        for rec in out_df.to_dict(orient="records"):
            yield rec

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
            | "Parse JSON" >> beam.ParDo(ReadJsonFlexibleAsRows())
            | "Normalize fields" >> beam.ParDo(NormalizeForProcessing(pool_over))
            | "Key by pool_over" >> beam.Map(lambda r: (r[pool_over], r))
            | "GroupByKey" >> beam.GroupByKey()
            | "Process groups" >> beam.ParDo(ProcessGroup(method))
            | "Log" >> beam.Map(print)    # Debug step
        )
