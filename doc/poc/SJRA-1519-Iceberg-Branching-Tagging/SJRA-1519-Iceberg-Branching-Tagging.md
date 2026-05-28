# SJRA-1519 — POC: Iceberg Branching and Tagging

## 1. Goals

- Evaluate the branching and tagging feature of Iceberg by validating what is possible and what is not.
- Evaluate the StarRocks support for branching and tagging in Iceberg. What can be done from StarRocks using Iceberg as an external catalog.

## 2. Background

- Table metadata maintains a snapshot log. Each snapshot represents a state of the table at a given point in time.
- Each write/update/delete/upsert/compaction in Iceberg produces a new snapshot (keeping the old data and metadata).
- You can manage the lifecycle of snapshots using specific procedures. (https://iceberg.apache.org/docs/nightly/spark-procedures/#expire_snapshots)
- Official documentation: https://iceberg.apache.org/docs/latest/branching/#overview

### 2.1 Iceberg Branching and Tagging

- A **branch** is a *mutable* named reference. Writes targeted at a branch advance only that branch's pointer; other branches and `main` are untouched. This is the Iceberg analogue of a git branch. 
- A **tag** is an *immutable* named reference — it pins one specific snapshot and never moves. Tags are the Iceberg analogue of a git tag / release marker. 

The `main` branch is the primary, default branch of an Iceberg table. There are no default tags.

> **Note**
> 
> There exist several lifecycle management options for branches and tags, but they are not detailed here.
>
> Check the official docs.

```
Snapshot log
                                                                  
        [tag: v1]             [tag: v2]                          
            │                     │                              
            ▼                     ▼                              
   ●────────●──────────●──────────●──────────●  ◄── [branch: main]     
   s1       s2         s3         s4         s5                   
                       │                                          
                       └──────────●──────────●  ◄── [branch: feature]    
                                  s6         s7                   
```

### 2.2 StarRocks support for Iceberg branching and tagging

It's possible to use StarRocks to query named refs: 

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc$refs;

name    |type  |snapshot_id        |max_reference_age_in_ms|min_snapshots_to_keep|max_snapshot_age_in_ms|
--------+------+-------------------+-----------------------+---------------------+----------------------+
v1.1.0  |TAG   |5463705435512831167|                       |                     |                      |
branch_A|BRANCH| 808920279501742852|                       |                     |                      |
branch_B|BRANCH|5463705435512831167|                       |                     |                      |
main    |BRANCH|5619325074383141912|                       |                     |                      |
```

---

## 3. Setup

- Using current `radiant-portal-sandbox` environment. 
- Using `pyiceberg` version `0.11.0` and Python `3.13.5` to run Iceberg operations.

## 4. Experimentation

#### 4.1.1 Basic

Creating a table and injecting data is done through the `create_basic_table` function in `iceberg_branching_tagging.py`. 

It creates a table with the following schema:

```
ARROW_SCHEMA = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("chromosome", pa.string(), nullable=False),
    pa.field("start", pa.int64(), nullable=True),
    pa.field("ref", pa.string(), nullable=True),
    pa.field("alt", pa.string(), nullable=True),
    pa.field("sample", pa.string(), nullable=True),
])
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc$refs;

name|type  |snapshot_id        |max_reference_age_in_ms|min_snapshots_to_keep|max_snapshot_age_in_ms|
----+------+-------------------+-----------------------+---------------------+----------------------+
main|BRANCH|2441523098581455863|                       |                     |                      |
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'main';

id|chromosome|start|ref|alt|sample  |
--+----------+-----+---+---+--------+
 1|chr1      | 1000|A  |T  |sample_a|
 2|chr1      | 2000|G  |C  |sample_b|
 3|chr2      | 3000|T  |G  |sample_a|
```

Then, we create a new branch `branch_A` from `main`:

```
def create_branch(table: Table, snapshot_id: int, branch_name: str) -> None:
    table.manage_snapshots().create_branch(
        snapshot_id=snapshot_id,
        branch_name=branch_name,
    ).commit()

create_branch(table=table, snapshot_id=table.refs()["main"].snapshot_id, branch_name="branch_A")
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc$refs;

name    |type  |snapshot_id        |max_reference_age_in_ms|min_snapshots_to_keep|max_snapshot_age_in_ms|
--------+------+-------------------+-----------------------+---------------------+----------------------+
branch_A|BRANCH|2441523098581455863|                       |                     |                      |
main    |BRANCH|2441523098581455863|                       |                     |                      |
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'branch_A';

id|chromosome|start|ref|alt|sample  |
--+----------+-----+---+---+--------+
 1|chr1      | 1000|A  |T  |sample_a|
 2|chr1      | 2000|G  |C  |sample_b|
 3|chr2      | 3000|T  |G  |sample_a|
```

#### 4.1.2 Adding data in a branch

```
def append_rows(table, branch_name: str, entries: pa.Table):
    table.append(entries, branch=branch_name)
    table.refresh()

new_data = pa.table({
      "id": [4, 5, 6],
      "chromosome": ["chr4", "chr5", "chr6"],
      "start": [4000, 5000, 6000],
      "ref": ["A", "G", "T"],
      "alt": ["TT", "CC", "GG"],
      "sample": ["sample_b", "sample_a", "sample_b"],
   },
   schema=ARROW_SCHEMA,
)
append_rows(table, "branch_A", new_data)
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc$refs;

name    |type  |snapshot_id        |max_reference_age_in_ms|min_snapshots_to_keep|max_snapshot_age_in_ms|
--------+------+-------------------+-----------------------+---------------------+----------------------+
branch_A|BRANCH|8501257248567957525|                       |                     |                      |
main    |BRANCH|2441523098581455863|                       |                     |                      |
```

(Notice the `snapshot_id` is now different for `branch_A` and `main`)

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'branch_A';

id|chromosome|start|ref|alt|sample  |
--+----------+-----+---+---+--------+
 1|chr1      | 1000|A  |T  |sample_a|
 2|chr1      | 2000|G  |C  |sample_b|
 3|chr2      | 3000|T  |G  |sample_a|
 4|chr4      | 4000|A  |TT |sample_b|
 5|chr5      | 5000|G  |CC |sample_a|
 6|chr6      | 6000|T  |GG |sample_b|
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'main';

id|chromosome|start|ref|alt|sample  |
--+----------+-----+---+---+--------+
 3|chr2      | 3000|T  |G  |sample_a|
 1|chr1      | 1000|A  |T  |sample_a|
 2|chr1      | 2000|G  |C  |sample_b|
```

#### 4.1.3 Creating a branch from a branch

```
create_branch(table, snapshot_id=table.refs()["branch_A"].snapshot_id, branch_name="branch_B")
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc$refs;

name    |type  |snapshot_id        |max_reference_age_in_ms|min_snapshots_to_keep|max_snapshot_age_in_ms|
--------+------+-------------------+-----------------------+---------------------+----------------------+
branch_A|BRANCH|5717039511397107282|                       |                     |                      |
branch_B|BRANCH|5717039511397107282|                       |                     |                      |
main    |BRANCH|3915552914707820610|                       |                     |                      |
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'branch_B';

id|chromosome|start|ref|alt|sample  |
--+----------+-----+---+---+--------+
 6|chr6      | 6000|T  |GG |sample_b|
 5|chr5      | 5000|G  |CC |sample_a|
 4|chr4      | 4000|A  |TT |sample_b|
 3|chr2      | 3000|T  |G  |sample_a|
 1|chr1      | 1000|A  |T  |sample_a|
 2|chr1      | 2000|G  |C  |sample_b|
```

#### 4.1.4 Add new data to `main` 

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'main';

id|chromosome|start|ref|alt|sample     |
--+----------+-----+---+---+-----------+
20|chr20     |20000|C  |GG |sample_main|
 1|chr1      | 1000|A  |T  |sample_a   |
 2|chr1      | 2000|G  |C  |sample_b   |
 3|chr2      | 3000|T  |G  |sample_a   |
```

(Other tables unchanged)

#### 4.1.5 Renaming a column

```
with table.update_schema() as update:
   update.rename_column("sample", "sample_id")
```

> ⚠️ Difference in behaviour between StarRocks and PyIceberg 

When scanning using `pyiceberg` it correctly matches the snapshot with its schema version:

```
Branch: branch_B
 id chromosome  start ref alt   sample
  4       chr4   4000   A  TT sample_b
  5       chr5   5000   G  CC sample_a
  6       chr6   6000   T  GG sample_b
  1       chr1   1000   A   T sample_a
  2       chr1   2000   G   C sample_b
  3       chr2   3000   T   G sample_a
```

However, in StarRocks, schema is handled at a table level:

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'branch_B';

id|chromosome|start|ref|alt|sample_id|
--+----------+-----+---+---+---------+
 5|chr5      | 5000|G  |CC |sample_a |
 4|chr4      | 4000|A  |TT |sample_b |
 1|chr1      | 1000|A  |T  |sample_a |
 2|chr1      | 2000|G  |C  |sample_b |
 6|chr6      | 6000|T  |GG |sample_b |
 3|chr2      | 3000|T  |G  |sample_a |
```

This is more dramatic when we drop columns:

```
with table.update_schema() as update:
  update.delete_column("sample_id")
```

With `pyiceberg`, it correctly uses the schema version of the snapshot to read the data:

```
Branch: main
 id chromosome  start ref alt      sample
 20      chr20  20000   C  GG sample_main
  1       chr1   1000   A   T    sample_a
  2       chr1   2000   G   C    sample_b
  3       chr2   3000   T   G    sample_a
```

With StarRocks, we are losing the column for all branches:

```
id|chromosome|start|ref|alt|
--+----------+-----+---+---+
 3|chr2      | 3000|T  |G  |
20|chr20     |20000|C  |GG |
 1|chr1      | 1000|A  |T  |
 2|chr1      | 2000|G  |C  |
```

#### 4.1.6 Fast-Forward of branches

Fast-forwarding a branch means moving the current snapshot of one branch to the latest snapshot of another.

> **Note**:
> 
> Currently, there's a limitation in `pyiceberg` that doesn't allow fast-forwarding a branch other than the `main` branch. 
> 
> However, this doesn't seem to be the case in the Spark/Java implementations based on the official documentation: 
> https://iceberg.apache.org/docs/latest/spark-procedures/?h=fas#fast_forward
> 
> Also, the `rollback_to_snapshot` function of `pyiceberg` uses `set_current_snapshot` under the hood, but with the caveat that the snapshot needs to be an ancestor of the current snapshot.

```
# Fast-forward (main only)
table.manage_snapshots().set_current_snapshot(table.refs()["branch_A"].snapshot_id).commit()
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'main';

id|chromosome|start|ref|alt|sample  |
--+----------+-----+---+---+--------+
 6|chr6      | 6000|T  |GG |sample_b|
 5|chr5      | 5000|G  |CC |sample_a|
 1|chr1      | 1000|A  |T  |sample_a|
 2|chr1      | 2000|G  |C  |sample_b|
 4|chr4      | 4000|A  |TT |sample_b|
 3|chr2      | 3000|T  |G  |sample_a|
```

Rollback to an invalid snapshot (not an ancestor) will fail: 

```
table.manage_snapshots().rollback_to_snapshot(table.refs()["branch_B"].snapshot_id).commit()
```

```
ValueError: Cannot roll back to snapshot, not an ancestor of the current state: 4614687712906166207
```

Rollback to a valid snapshot (original `main` snapshot) will succeed:

```
original_main_snapshot_id = table.refs()["main"].snapshot_id

...

table.manage_snapshots().rollback_to_snapshot(original_main_snapshot_id).commit()
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'main';

id|chromosome|start|ref|alt|sample_id|
--+----------+-----+---+---+---------+
 1|chr1      | 1000|A  |T  |sample_a |
 2|chr1      | 2000|G  |C  |sample_b |
 3|chr2      | 3000|T  |G  |sample_a |
```

#### 4.1.7 Real-world scenario

Using the Sandbox's `clinvar` table: 

```
>>> SELECT * FROM radiant_iceberg_catalog.radiant.clinvar$refs;

name|type  |snapshot_id        |max_reference_age_in_ms|min_snapshots_to_keep|max_snapshot_age_in_ms|
----+------+-------------------+-----------------------+---------------------+----------------------+
main|BRANCH|3212848341756229121|                       |                     |                      |
```

```
>>> SELECT COUNT(1) FROM radiant_iceberg_catalog.radiant.clinvar;

count(1)|
--------+
    3543|
```

Create a new `audit` branch on the `clinvar` table: 

```
clinvar_table = get_table(".".join(["radiant", "clinvar"]), catalog)
create_branch(clinvar_table, clinvar_table.refs()["main"].snapshot_id, "audit")
```

```
>>> SELECT * FROM radiant_iceberg_catalog.radiant.clinvar$refs;

name |type  |snapshot_id        |max_reference_age_in_ms|min_snapshots_to_keep|max_snapshot_age_in_ms|
-----+------+-------------------+-----------------------+---------------------+----------------------+
audit|BRANCH|3212848341756229121|                       |                     |                      |
main |BRANCH|3212848341756229121|                       |                     |                      |
```

```
>>> SELECT COUNT(1) FROM radiant_iceberg_catalog.radiant.clinvar VERSION AS OF 'audit';

count(1)|
--------+
    3543|
```


Inject 100 new rows inside the `audit` branch of the `clinvar` table:

```
first_100 = clinvar_table.scan(limit=100).to_arrow()
append_rows(clinvar_table, "audit", first_100)
```

```
>>> SELECT COUNT(1) FROM radiant_iceberg_catalog.radiant.clinvar VERSION AS OF 'audit';

count(1)|
--------+
    3643|
```

While `main` remains unchanged:

```
>>> SELECT COUNT(1) FROM radiant_iceberg_catalog.radiant.clinvar VERSION AS OF 'main';

count(1)|
--------+
    3543|
```

Now, we can fast-forward `main` to `audit`:
(https://py.iceberg.apache.org/reference/pyiceberg/table/update/snapshot/#pyiceberg.table.update.snapshot.ManageSnapshots.set_current_snapshot)

```
clinvar_table.manage_snapshots().set_current_snapshot(clinvar_table.refs()["audit"].snapshot_id).commit()
```

```
>>> SELECT COUNT(1) FROM radiant_iceberg_catalog.radiant.clinvar VERSION AS OF 'main';
count(1)|
--------+
    3643|
```

### 4.2 Tagging

#### 4.2.1 Basic

```
def create_tag(table: Table, snapshot_id: int, tag_name: str) -> None:
    table.manage_snapshots().create_tag(
        snapshot_id=snapshot_id,
        tag_name=tag_name
    ).commit()
    
create_tag(table, snapshot_id=table.refs()["branch_B"].snapshot_id, tag_name="v1.1.0")
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'v1.1.0' ORDER BY id;

id|chromosome|start|ref|alt|sample_id|
--+----------+-----+---+---+---------+
 1|chr1      | 1000|A  |T  |sample_a |
 2|chr1      | 2000|G  |C  |sample_b |
 3|chr2      | 3000|T  |G  |sample_a |
 4|chr4      | 4000|A  |TT |sample_b |
 5|chr5      | 5000|G  |CC |sample_a |
 6|chr6      | 6000|T  |GG |sample_b |
 7|chr7      | 7000|A  |TT |sample_c |
```

#### 4.2.2 Deleting data

Deleting data from `branch_B` does not impact the data in the tag `v1.1.0`:

```
table.delete(delete_filter="id = 1", branch="branch_B")
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'branch_B' ORDER BY id;

id|chromosome|start|ref|alt|sample_id|
--+----------+-----+---+---+---------+
 2|chr1      | 2000|G  |C  |sample_b |
 3|chr2      | 3000|T  |G  |sample_a |
 4|chr4      | 4000|A  |TT |sample_b |
 5|chr5      | 5000|G  |CC |sample_a |
 6|chr6      | 6000|T  |GG |sample_b |
 7|chr7      | 7000|A  |TT |sample_c |
```

```
>>> SELECT * FROM radiant_iceberg_catalog.sjra_1519_poc.poc VERSION AS OF 'v1.1.0' ORDER BY id;

id|chromosome|start|ref|alt|sample_id|
--+----------+-----+---+---+---------+
 1|chr1      | 1000|A  |T  |sample_a |
 2|chr1      | 2000|G  |C  |sample_b |
 3|chr2      | 3000|T  |G  |sample_a |
 4|chr4      | 4000|A  |TT |sample_b |
 5|chr5      | 5000|G  |CC |sample_a |
 6|chr6      | 6000|T  |GG |sample_b |
 7|chr7      | 7000|A  |TT |sample_c |
```

## 5. Discussion

### 5.1 Limitation of StarRocks inferring schema versions

**Problem**:

In Spark, when querying a specific snapshot in Iceberg, it will use the snapshot's schema to read the data (source: https://iceberg.apache.org/docs/latest/spark-queries/?h=schema#schema-selection-in-time-travel-queries)

The current limitations in StarRocks prevents the usage of branches alone for a Write-Audit-Publish (WAP) workflow. 
StarRocks always infers the latest schema and applies it to any snapshot. 

Source: https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_timetravel/#query-with-time-travel

- Not mention of schema on this page. 
- Validated with CelerData team that this is a limitation of current versions of StarRocks. (Planned work in the future to better integrate with Iceberg)

**Workaround**:

A potential solution would be to create a new table for each "major" versions requiring a schema change. 
This creates additional pressure on the storage system, but would allow keeping track of different schemas.

> **Important note**:
> 
> The above are facts as of 2026-05-27. StarRocks team is aware of the issue and is actively working on a solution.
> It might be worthwhile to revisit the issue in the future to see if schemas can be inferred at a snapshot level, which would allow using branches for WAP without the need of creating multiple tables.

### 5.2 Limitation of PyIceberg for fast-forwarding branches other than `main`

**Problem**:

Currently, there's no implementation of the fast-forward mechanism in the PyIceberg library. 

**Workaround**:

Fast-forwarding is achievable on the main branch using the `manage_snapshots().set_current_snapshot()` method, but there's no way to fast-forward other branches.

Source: https://py.iceberg.apache.org/reference/pyiceberg/table/update/snapshot/#pyiceberg.table.update.snapshot.ManageSnapshots.set_current_snapshot

```
update, requirement = self._transaction._set_ref_snapshot(
    snapshot_id=target_snapshot_id,
    ref_name=MAIN_BRANCH,
    type=SnapshotRefType.BRANCH,
)
```

---

## X. References

- [Iceberg — Branching and Tagging](https://iceberg.apache.org/docs/latest/branching/)
- [Iceberg — Spark DDL: branches & tags](https://iceberg.apache.org/docs/latest/spark-ddl/#branching-and-tagging-ddl)
- [Iceberg — Spark procedures: `fast_forward`, `expire_snapshots`](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Iceberg — Schema evolution (schema-on-read)](https://iceberg.apache.org/docs/latest/evolution/#schema-evolution)
- [PyIceberg — Snapshot management (branches/tags)](https://py.iceberg.apache.org/api/#snapshot-management)
- [PyIceberg — REST catalog configuration](https://py.iceberg.apache.org/configuration/#rest-catalog)
- [Apache Polaris — documentation](https://polaris.apache.org/)
- [StarRocks — Iceberg catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/)
- [StarRocks — Time travel with Iceberg (`FOR VERSION/TIMESTAMP AS OF`)](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/#time-travel)
