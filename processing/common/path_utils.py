from __future__ import annotations
from pathlib import PurePosixPath


def file_stem(file_name: str) -> str:
    """
    BCBS001_837P_20260312.csv -> BCBS001_837P_20260312
    """
    if not file_name or "." not in file_name:
        raise ValueError(f"Invalid file_name: {file_name}")
    return file_name.rsplit(".", 1)[0]


# ── Raw ──────────────────────────────────────────────────────
def raw_input_path(file_name: str) -> str:
    return f"s3a://healthcare-raw/{file_name}"


# ── Cleansed ─────────────────────────────────────────────────
def cleansed_output_path(file_name: str) -> str:
    return f"s3a://healthcare-cleansed/claims/{file_stem(file_name)}/"


def quarantine_output_path(file_name: str) -> str:
    return f"s3a://healthcare-quarantine/claims/{file_stem(file_name)}/"


# ── Transformed ──────────────────────────────────────────────
def transformed_output_path(file_name: str) -> str:
    return f"s3a://healthcare-transformed/claims/{file_stem(file_name)}/"


def payer_summary_output_path(file_name: str) -> str:
    return f"s3a://healthcare-transformed/summaries/payer/{file_stem(file_name)}/"


def cpt_summary_output_path(file_name: str) -> str:
    return f"s3a://healthcare-transformed/summaries/cpt/{file_stem(file_name)}/"


# ── Curated ──────────────────────────────────────────────────
def curated_member_summary_path(file_name: str) -> str:
    return f"s3a://healthcare-curated/member_summary/{file_stem(file_name)}/"


def curated_payer_summary_path(file_name: str) -> str:
    return f"s3a://healthcare-curated/payer_summary/{file_stem(file_name)}/"


def curated_provider_summary_path(file_name: str) -> str:
    return f"s3a://healthcare-curated/provider_summary/{file_stem(file_name)}/"