# FileMeta — Parse Once, Pass Everywhere

## What is it

The FileMeta pattern is a software design principle applied to pipeline filename handling: **parse and validate input at the system boundary once, represent it as a typed object, and pass that object everywhere downstream**. Never pass raw strings.

This is an application of the **Parse, Don't Validate** principle from functional programming: instead of checking a string is valid at each point of use, transform it into a type that *can only exist if it's valid*. If you have a `FileMeta`, you know the filename was valid — no further checking needed.

A **frozen dataclass** (`@dataclass(frozen=True)`) is the right Python construct for this:
- Immutable after construction — no accidental mutation
- Hashable — can be used as a dict key or in a set
- Auto-generates `__repr__` and `__eq__` — useful for logging and testing
- Named fields — self-documenting, no positional argument confusion

---

## Why we use it here

Every task in the pipeline needs to know the client code, file type, environment, date, and sequence number — to construct MinIO paths, Airflow log messages, and PostgreSQL records. Without FileMeta, each component would either:

1. Re-parse the filename string independently (duplicated parsing logic, duplicated validation)
2. Receive raw string arguments for each field (error-prone, easy to pass arguments in the wrong order)
3. Do no validation at all (bad data silently propagates)

FileMeta solves all three: parse once at the entry point, validate once at the entry point, pass a typed object everywhere else. If `parse_filename()` succeeds, every downstream component can trust the data.

---

## How it's implemented

**The dataclass (`processing/common/path_utils.py`):**

```python
@dataclass(frozen=True)
class FileMeta:
    file_name:   str   # BCBS001_837P_PROD_20260312_001.csv
    client_code: str   # BCBS001
    file_type:   str   # 837P
    env:         str   # PROD
    date:        str   # 20260312
    sequence:    str   # 001

    @property
    def stem(self) -> str:
        return self.file_name.rsplit(".", 1)[0]   # BCBS001_837P_PROD_20260312_001

    @property
    def partition(self) -> str:
        return f"{self.client_code}/{self.file_type}/{self.env}/{self.date}_{self.sequence}"

    @property
    def is_prod(self) -> bool:
        return self.env == "PROD"
```

**The parser (`processing/common/path_utils.py`):**

```python
_FILENAME_RE = re.compile(
    r"^(?P<client_code>[A-Z0-9]{3,10})"
    r"_(?P<file_type>837P|837I|835|MEMBER|PHARMACY|CROSSWALK)"
    r"_(?P<env>PROD|TEST|SYS1|SYS2|UAT1|UAT2)"
    r"_(?P<date>\d{8})"
    r"_(?P<sequence>\d{3})"
    r"\.csv$"
)

def parse_filename(file_name: str) -> FileMeta:
    match = _FILENAME_RE.match(file_name)
    if not match:
        raise ValueError(
            f"Invalid filename: '{file_name}'\n"
            f"Expected: {{CLIENT_CODE}}_{{FILE_TYPE}}_{{ENV}}_{{DATE}}_{{SEQUENCE}}.csv"
        )
    groups = match.groupdict()
    
    # Validate real calendar date (regex only validates 8 digits, not a real date)
    try:
        datetime.strptime(groups["date"], "%Y%m%d")
    except ValueError:
        raise ValueError(f"Invalid date '{groups['date']}' in filename '{file_name}'.")
    
    return FileMeta(file_name=file_name, **groups)
```

**Two validation layers:**

1. **Regex** — validates structure, valid enum values for FILE_TYPE and ENV, 8-digit date, 3-digit sequence
2. **Calendar date check** — `datetime.strptime("20261399", "%Y%m%d")` raises `ValueError` for impossible dates. Regex `\d{8}` would accept `20261399`.

**How it's used across all entry points:**

In `file_watcher.py`:
```python
try:
    meta = parse_filename(local_path.name)   # raises ValueError if invalid
except ValueError as e:
    upload_invalid_file(local_path, str(e))  # quarantine the file
    return

upload_valid_file(meta, local_path)   # meta passed to path functions
trigger_dag(meta)
```

In `claims_cleanse.py`:
```python
args = parse_args()
meta = parse_filename(args.file_name)   # validate at job entry point

input_path      = raw_input_path(meta)      # no string construction elsewhere
clean_path      = cleansed_output_path(meta)
quarantine_path = quarantine_output_path(meta)
```

In the Airflow DAG — every Python task re-parses:
```python
def get_meta(**context) -> FileMeta:
    file_name = context["dag_run"].conf.get("file_name")
    return parse_filename(file_name)   # self-contained, re-runnable in isolation
```

**Path functions — all centralized:**

```python
def raw_input_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-raw/{meta.partition}/{meta.file_name}"

def cleansed_output_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-cleansed/claims/{meta.partition}/"

def transformed_output_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-transformed/claims/{meta.partition}/"
```

If the zone structure changes, one file changes — not every job that constructs a path.

---

## Interview Q&A

**Q: Why a frozen dataclass instead of a plain dict or namedtuple?**

> A frozen dataclass gives you named fields (self-documenting, no positional confusion), immutability (no accidental mutation after construction), computed properties (`partition`, `is_prod`), and auto-generated `__repr__` and `__eq__`. A plain dict is mutable and has no type contract. A namedtuple is immutable but doesn't support computed properties or default values cleanly. Frozen dataclass is the right tool for an immutable value object in Python.

**Q: Why re-parse `FileMeta` in every Airflow task instead of passing it via XCom?**

> Each task should be independently re-runnable. If you pass FileMeta via XCom, re-running `validate_cleansed_output` in isolation requires `check_file_exists` to have run first and pushed the value. By re-parsing from `dag_run.conf` — which is always available for the lifetime of the DAG run — any task can be cleared and re-run without dependencies on sibling task output. The parsing cost is negligible; the operational flexibility is significant.

**Q: What's the "Parse, Don't Validate" principle?**

> Instead of passing a string everywhere and checking its validity at each use site, parse it once at the boundary and return a type that *guarantees* the data is valid. If you have a `FileMeta` object, you know the filename was valid when you constructed it — no need to validate again. This pushes failure to the entry point where it belongs and removes defensive checks from the middle of the system. The alternative — validating strings repeatedly — is where bugs hide: one call site checks, another doesn't.

**Q: How do you handle the case where a file_type or ENV value is added in the future?**

> `VALID_FILE_TYPES` and `VALID_ENVS` are defined as sets at the top of `path_utils.py`. The regex is also defined there. Adding a new file type means updating both the set and the regex in one place. Because all parsing flows through `parse_filename()`, the change propagates everywhere automatically — no other files need to change.
