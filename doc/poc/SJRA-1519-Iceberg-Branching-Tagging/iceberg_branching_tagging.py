#!/usr/bin/env python3
"""
This script is for reference only.
See SJRA-1519-Iceberg-Branching-Tagging.md for experimental notes and discussion.
"""

import os
import pyarrow as pa
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.refs import SnapshotRefType
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

POLARIS_URI = os.environ.get("POLARIS_URI", "http://localhost:8181/api/catalog")
CLIENT_ID = os.environ.get("POLARIS_CLIENT_ID", "root")
CLIENT_SECRET = os.environ.get("POLARIS_CLIENT_SECRET", "password")
REALM = os.environ.get("POLARIS_REALM", "radiant")
CATALOG_NAME = os.environ.get("POLARIS_CATALOG", "polaris")
SCOPE = "PRINCIPAL_ROLE:ALL"

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "password")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")

NAMESPACE = "sjra_1519_poc"
TABLE = "poc"
FQN = (NAMESPACE, TABLE)

ARROW_SCHEMA = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("chromosome", pa.string(), nullable=False),
    pa.field("start", pa.int64(), nullable=True),
    pa.field("ref", pa.string(), nullable=True),
    pa.field("alt", pa.string(), nullable=True),
    pa.field("sample", pa.string(), nullable=True),
])

ARROW_UPDATED_SCHEMA = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("chromosome", pa.string(), nullable=False),
    pa.field("start", pa.int64(), nullable=True),
    pa.field("ref", pa.string(), nullable=True),
    pa.field("alt", pa.string(), nullable=True),
    pa.field("sample_id", pa.string(), nullable=True),
])

ARROW_UPDATED_SCHEMA_NO_SAMPLE_ID = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("chromosome", pa.string(), nullable=False),
    pa.field("start", pa.int64(), nullable=True),
    pa.field("ref", pa.string(), nullable=True),
    pa.field("alt", pa.string(), nullable=True),
])

def get_polaris_catalog():
    return load_catalog(
        "polaris",
        **{
            "type": "rest",
            "uri": POLARIS_URI,
            "credential": f"{CLIENT_ID}:{CLIENT_SECRET}",
            "scope": SCOPE,
            "warehouse": CATALOG_NAME,
            "header.Polaris-Realm": REALM,
            "access-delegation": "",
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.path-style-access": "true",
            "s3.region": S3_REGION,
        },
    )


def get_table(name: str, catalog: Catalog) -> Table | None:
    if catalog.table_exists(name):
        return catalog.load_table(identifier=name)
    return None

def create_basic_table():
    catalog = get_polaris_catalog()

    session = getattr(catalog, "_session", None)
    if session is not None:
        session.headers.pop("X-Iceberg-Access-Delegation", None)

    catalog.create_namespace_if_not_exists(NAMESPACE)

    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "chromosome", StringType(), required=True),
        NestedField(3, "start", LongType(), required=False),
        NestedField(4, "ref", StringType(), required=False),
        NestedField(5, "alt", StringType(), required=False),
        NestedField(6, "sample", StringType(), required=False),
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="chromosome")
    )

    if catalog.table_exists(FQN):
        catalog.drop_table(FQN)

    table = catalog.create_table(
        identifier=FQN,
        schema=schema,
        partition_spec=partition_spec,
        properties={
            "format-version": "2",
            "write.parquet.compression-codec": "zstd",
        },
    )
    rows = pa.table(
        {
            "id": [1, 2, 3],
            "chromosome": ["chr1", "chr1", "chr2"],
            "start": [1000, 2000, 3000],
            "ref": ["A", "G", "T"],
            "alt": ["T", "C", "G"],
            "sample": ["sample_a", "sample_b", "sample_a"],
        },
        schema=ARROW_SCHEMA,
    )
    table.append(rows, snapshot_properties={"dataset_version": "1.0", "gencode_version": "105"})

def create_tag(table: Table, snapshot_id: int, tag_name: str, max_ref_age_ms: int | None = None) -> None:
    table.manage_snapshots().create_tag(
        snapshot_id=snapshot_id,
        tag_name=tag_name,
        max_ref_age_ms=max_ref_age_ms,
    ).commit()

def remove_tag(table: Table, tag_name: str) -> None:
    table.manage_snapshots().remove_tag(tag_name=tag_name).commit()

def create_branch(table: Table, snapshot_id: int, branch_name: str) -> None:
    table.manage_snapshots().create_branch(
        snapshot_id=snapshot_id,
        branch_name=branch_name,
    ).commit()

def delete_branch(table: Table, branch_name: str) -> None:
    table.manage_snapshots().remove_branch(
        branch_name=branch_name,
    ).commit()

def get_branches(table: Table):
    return [
        name
        for name, data in table.refs().items()
        if data.snapshot_ref_type == SnapshotRefType.BRANCH
    ]

def table_scan(table: Table, branch_name: str) -> None:
    snapshot_id = None
    if branch_name:
        if branch_name not in get_branches(table):
            return
        snapshot_id = table.refs()[branch_name].snapshot_id

    scan = table.scan(snapshot_id=snapshot_id).to_arrow()
    print(scan.to_pandas().to_string(index=False))


def append_rows(table, branch_name: str, entries: pa.Table, snapshot_properties: dict | None = None) -> None:
    table.append(entries, snapshot_properties=snapshot_properties, branch=branch_name)
    table.refresh()


def checkpoint_branches(table, caption, inspect: bool = False) -> None:
    print(f"\n{caption}")
    branches = get_branches(table)
    for branch in branches:
        print(f"Branch: {branch}")
        table_scan(table, branch)

    if inspect:
        print(f"\nInspections:")
        print(table.inspect.snapshots())
        print(table.inspect.partitions())
        print(table.inspect.entries())


def clinvar_scenario():
    catalog = get_polaris_catalog()

    session = getattr(catalog, "_session", None)
    if session is not None:
        session.headers.pop("X-Iceberg-Access-Delegation", None)

    clinvar_table = get_table(".".join(["radiant", "clinvar"]), catalog)
    if not clinvar_table:
        return

    # Create 'audit' branch from main (skip if already present)
    if "audit" not in get_branches(clinvar_table):
        create_branch(clinvar_table, clinvar_table.refs()["main"].snapshot_id, "audit")
        clinvar_table.refresh()

    # Read first 100 rows from clinvar (main) and re-inject into the 'audit' branch
    first_100 = clinvar_table.scan(limit=100).to_arrow()
    append_rows(clinvar_table, "audit", first_100, snapshot_properties={"dataset_version": "1.0", "gencode_version": "105"})

    checkpoint_branches(clinvar_table, caption="After re-injecting first 100 rows into 'audit' branch..:")

    clinvar_table.manage_snapshots().set_current_snapshot(clinvar_table.refs()["audit"].snapshot_id).commit()


def basic_scenario():
    create_basic_table()
    catalog = get_polaris_catalog()

    session = getattr(catalog, "_session", None)
    if session is not None:
        session.headers.pop("X-Iceberg-Access-Delegation", None)

    table = get_table(".".join(FQN), catalog)

    if not table:
        return

    # --- Create a branch: 'branch_A'

    original_main_snapshot_id = table.refs()["main"].snapshot_id

    checkpoint_branches(table, caption="Before creating 'branch_A' branch..:")
    create_branch(table, table.refs()["main"].snapshot_id, "branch_A")

    checkpoint_branches(table, caption="After creating 'branch_A' branch..:")
    new_data = pa.table(
        {
            "id": [4, 5, 6],
            "chromosome": ["chr4", "chr5", "chr6"],
            "start": [4000, 5000, 6000],
            "ref": ["A", "G", "T"],
            "alt": ["TT", "CC", "GG"],
            "sample": ["sample_b", "sample_a", "sample_b"],
        },
        schema=ARROW_SCHEMA,
    )
    append_rows(table, "branch_A", new_data, snapshot_properties={"dataset_version": "2.0", "gencode_version": "105"})

    print("\nAfter adding new data to 'branch_A' branch..:")
    checkpoint_branches(table, caption="After adding new data to 'branch_A' branch..:")

    # Fast-forward main
    table.manage_snapshots().set_current_snapshot(table.refs()["branch_A"].snapshot_id).commit()

    checkpoint_branches(table, caption="After fast-forwarding main to branch_A..:")

    # Create a 'branch_B', based on the 'branch_A' snapshot_id
    create_branch(table, snapshot_id=table.refs()["branch_A"].snapshot_id, branch_name="branch_B")
    checkpoint_branches(table, caption="After adding 'branch_B' based on 'branch_A':")

    new_data = pa.table(
        {
            "id": [20],
            "chromosome": ["chr20"],
            "start": [20000],
            "ref": ["C"],
            "alt": ["GG"],
            "sample": ["sample_main"],
        },
        schema=ARROW_SCHEMA,
    )
    append_rows(table, "main", new_data, snapshot_properties={"dataset_version": "3.0", "gencode_version": "105"})
    checkpoint_branches(table, caption="After adding new data to 'main' branch..:")

    # Add a column in branch_B
    with table.update_schema() as update:
        update.rename_column("sample", "sample_id")

    table.refresh()
    checkpoint_branches(table, caption="After renaming 'sample' to 'sample_id' in 'branch_B':")

    new_data = pa.table(
        {
            "id": [7],
            "chromosome": ["chr7"],
            "start": [7000],
            "ref": ["A"],
            "alt": ["TT"],
            "sample_id": ["sample_c"],
        },
        schema=ARROW_UPDATED_SCHEMA,
    )
    append_rows(table, "branch_B", new_data, snapshot_properties={"dataset_version": "4.0", "gencode_version": "105"})
    checkpoint_branches(table, caption="After appending new data to 'branch_B' branch..:")

    # Delete a column in branch_B
    with table.update_schema() as update:
        update.delete_column("sample_id")

    table.refresh()
    checkpoint_branches(table, caption="After deleting `sample_id` from 'branch_B'..:")

    new_data = pa.table(
        {
            "id": [8],
            "chromosome": ["chr8"],
            "start": [8000],
            "ref": ["A"],
            "alt": ["TT"],
        },
        schema=ARROW_UPDATED_SCHEMA_NO_SAMPLE_ID,
    )
    append_rows(table, "branch_B", new_data, snapshot_properties={"dataset_version": "5.0", "gencode_version": "111"})
    checkpoint_branches(table, caption="After appending new 'deleted' data to 'branch_B' branch..:")

    create_tag(table, snapshot_id=table.refs()["branch_B"].snapshot_id, tag_name="v1.1.0")
    checkpoint_branches(table, caption="After tagging 'branch_B' branch to v1.1.0..:")

    # Delete some data in branch_B, should not cause the tag to have delete data as well
    table.delete(delete_filter="id = 1", branch="branch_B")
    table.refresh()
    checkpoint_branches(table, caption="After deleting id=1 from 'branch_B' branch..:")

    # Raises ValueError
    # table.manage_snapshots().rollback_to_snapshot(table.refs()["branch_B"].snapshot_id).commit()

    table.manage_snapshots().rollback_to_snapshot(original_main_snapshot_id).commit()
    checkpoint_branches(table, caption="After rolling back main:")



def main() -> None:
    # clinvar_scenario()
    basic_scenario()


if __name__ == "__main__":
    main()
