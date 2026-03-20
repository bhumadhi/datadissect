from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

# ── Valid Values ─────────────────────────────────────────────
VALID_FILE_TYPES = {"837P", "837I", "835", "MEMBER", "PHARMACY", "CROSSWALK"}
VALID_ENVS       = {"PROD", "TEST", "SYS1", "SYS2", "UAT1", "UAT2"}

# Regex: CLIENT_CODE_FILETYPE_ENV_DATE_SEQUENCE.csv
_FILENAME_RE = re.compile(
    r"^(?P<client_code>[A-Z0-9]{3,10})"
    r"_(?P<file_type>837P|837I|835|MEMBER|PHARMACY|CROSSWALK)"
    r"_(?P<env>PROD|TEST|SYS1|SYS2|UAT1|UAT2)"
    r"_(?P<date>\d{8})"
    r"_(?P<sequence>\d{3})"
    r"\.csv$"
)


# ── FileMeta ─────────────────────────────────────────────────
@dataclass(frozen=True)
class FileMeta:
    """Parsed, immutable representation of a validated filename."""
    file_name:   str   # BCBS001_837P_PROD_20260312_001.csv
    client_code: str   # BCBS001
    file_type:   str   # 837P
    env:         str   # PROD
    date:        str   # 20260312
    sequence:    str   # 001

    @property
    def stem(self) -> str:
        """BCBS001_837P_PROD_20260312_001"""
        return self.file_name.rsplit(".", 1)[0]

    @property
    def partition(self) -> str:
        """BCBS001/837P/PROD/20260312_001 — used as subfolder in all zones"""
        return f"{self.client_code}/{self.file_type}/{self.env}/{self.date}_{self.sequence}"

    @property
    def is_prod(self) -> bool:
        return self.env == "PROD"


def parse_filename(file_name: str) -> FileMeta:
    """
    Parse and validate a filename into a FileMeta.
    Raises ValueError with a clear message if the filename is invalid.

    Valid:   BCBS001_837P_PROD_20260312_001.csv
    Invalid: report.csv, BCBS001_UNKNOWN_20260312_001.csv
    """
    match = _FILENAME_RE.match(file_name)
    if not match:
        raise ValueError(
            f"Invalid filename: '{file_name}'\n"
            f"Expected: {{CLIENT_CODE}}_{{FILE_TYPE}}_{{ENV}}_{{DATE}}_{{SEQUENCE}}.csv\n"
            f"Example:  BCBS001_837P_PROD_20260312_001.csv\n"
            f"Valid FILE_TYPE: {sorted(VALID_FILE_TYPES)}\n"
            f"Valid ENV:       {sorted(VALID_ENVS)}"
        )

    groups = match.groupdict()

    # Validate DATE is a real calendar date
    try:
        datetime.strptime(groups["date"], "%Y%m%d")
    except ValueError:
        raise ValueError(
            f"Invalid date '{groups['date']}' in filename '{file_name}'. "
            f"Expected a valid YYYYMMDD date."
        )

    return FileMeta(
        file_name=file_name,
        client_code=groups["client_code"],
        file_type=groups["file_type"],
        env=groups["env"],
        date=groups["date"],
        sequence=groups["sequence"],
    )


# ── Raw ──────────────────────────────────────────────────────
def raw_input_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-raw/{meta.partition}/{meta.file_name}"


# ── Cleansed ─────────────────────────────────────────────────
def cleansed_output_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-cleansed/claims/{meta.partition}/"


def quarantine_output_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-quarantine/claims/{meta.partition}/"


# ── Transformed ──────────────────────────────────────────────
def transformed_output_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-transformed/claims/{meta.partition}/"


def payer_summary_output_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-transformed/summaries/payer/{meta.partition}/"


def cpt_summary_output_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-transformed/summaries/cpt/{meta.partition}/"


# ── Curated ──────────────────────────────────────────────────
def curated_member_summary_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-curated/member_summary/{meta.partition}/"


def curated_payer_summary_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-curated/payer_summary/{meta.partition}/"


def curated_provider_summary_path(meta: FileMeta) -> str:
    return f"s3a://healthcare-curated/provider_summary/{meta.partition}/"


# ── Scratch ──────────────────────────────────────────────────
def scratch_path(label: str, username: str = "default") -> str:
    """
    Temporary workspace for intermediate datasets.
    Namespaced by user and label to avoid collisions.
    Bucket has a 7-day TTL lifecycle policy — treat as throwaway.

    Example: s3a://healthcare-scratch/bhuwan/member_age_test/
    """
    return f"s3a://healthcare-scratch/{username}/{label}/"