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
    for each (key, rows):
      - Build DataFrame from rows
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

# ----------------------- Combine to Pandas JSON string ------------------------ #

class RecordsToSingleJsonString(beam.CombineFn):
    """
    Combine all records and return a single JSON string identical to:
        pd.DataFrame(records)
          .sort_values(by=item_id in natural order)
          .reset_index(drop=True)
          .to_json(double_precision=10)
    (orient='columns' by default) to match your expected output and ordering.
    """
    def create_accumulator(self) -> List[Dict]:
        return []

    def add_input(self, acc: List[Dict], rec: Dict) -> List[Dict]:
        acc.append(rec)
        return acc

    def merge_accumulators(self, accs: List[List[Dict]]) -> List[Dict]:
        merged: List[Dict] = []
        for a in accs:
            merged.extend(a)
        return merged

    def extract_output(self, acc: List[Dict]) -> str:
        df = pd.DataFrame(acc)

        # Ensure stable column order: item_id first if present
        if "item_id" in df.columns:
            # Natural sort for IDs like i1, i2, i10 (prefix + numeric suffix)
            s = df["item_id"].astype(str)
            prefix = s.str.replace(r"(\d+)$", "", regex=True)
            num = s.str.extract(r"(\d+)$", expand=False).fillna("-1").astype(int)

            df = (
                df.assign(_prefix=prefix, _num=num)
                  .sort_values(by=["_prefix", "_num", "item_id"], kind="mergesort")
                  .drop(columns=["_prefix", "_num"])
                  .reset_index(drop=True)
            )

            # Put item_id first
            cols = ["item_id"] + [c for c in df.columns if c != "item_id"]
            df = df[cols]
        else:
            # Still reset index for clean 0..n-1 indices
            df = df.reset_index(drop=True)

        # Single-line JSON (pandas default has no pretty printing)
        return df.to_json(double_precision=10)


# --------------------------------- Runner ------------------------------------ #

def run_beam(
    method: str,
    pool_over: str,
    input_path: str = "data.json",
    output_path: str = "output.json",
    direct_running_mode: str = "multi_threading",  # or 'multi_processing' (use with care for PyTorch)
    direct_num_workers: int = 4,
    remaining_args=None
) -> None:
    opts = PipelineOptions(
        remaining_args or [],
        runner="DirectRunner",  # or "DataflowRunner" for GCP
        save_main_session=True
    )
    direct_opts = opts.view_as(DirectOptions)
    direct_opts.direct_running_mode = direct_running_mode
    direct_opts.direct_num_workers = direct_num_workers

    with beam.Pipeline(options=opts) as p:
        rows = (
            p
            | "Match input" >> fileio.MatchFiles(input_path)
            | "Read files" >> fileio.ReadMatches()
            | "Read UTF8" >> beam.Map(lambda r: r.read_utf8())
            | "Parse JSON" >> beam.ParDo(ReadJsonFlexibleAsRows())
#            | "Normalize fields" >> beam.ParDo(NormalizeForProcessing(pool_over))
        )

        processed_records = (
            rows
            | "Key by pool_over" >> beam.Map(lambda r: (r[pool_over], r))
            | "GroupByKey" >> beam.GroupByKey()
            | "Process groups" >> beam.ParDo(ProcessGroup(method))
        )

        (
            processed_records
            | "To single JSON string" >> beam.CombineGlobally(RecordsToSingleJsonString()).without_defaults()
            | "Write output.json" >> beam.io.WriteToText(
                output_path,
                num_shards=1,
                shard_name_template="",  # exact filename (no sharding suffix)
            )
        )
