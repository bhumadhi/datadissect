"""
One-time MinIO setup: create all required buckets and upload reference data.
Run this after 'docker-compose up -d' whenever volumes are fresh (e.g. after rename or reset).

Usage:
    python3 scripts/init_minio.py
"""

from __future__ import annotations

import os
import sys

from minio import Minio

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

BUCKETS = [
    "healthcare-raw",
    "healthcare-cleansed",
    "healthcare-quarantine",
    "healthcare-transformed",
    "healthcare-curated",
    "healthcare-reference",
    "healthcare-scratch",
]

REFERENCE_FILES = {
    "cpt_codes.csv":   "data/reference/cpt_codes.csv",
    "icd10_codes.csv": "data/reference/icd10_codes.csv",
}


def main() -> None:
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    client = Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    print("Creating buckets...")
    for bucket in BUCKETS:
        if client.bucket_exists(bucket):
            print(f"  already exists: {bucket}")
        else:
            client.make_bucket(bucket)
            print(f"  created:        {bucket}")

    print("\nUploading reference data to healthcare-reference...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    for object_name, relative_path in REFERENCE_FILES.items():
        local_path = os.path.join(project_root, relative_path)
        if not os.path.exists(local_path):
            print(f"  WARNING: {local_path} not found — skipping")
            continue
        client.fput_object("healthcare-reference", object_name, local_path)
        print(f"  uploaded: {object_name}")

    print("\nAll MinIO buckets:", [b.name for b in client.list_buckets()])
    print("\nMinIO init complete ✅")


if __name__ == "__main__":
    main()
