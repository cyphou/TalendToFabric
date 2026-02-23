# Talend Job Complexity Assessment

## Scoring Criteria

Each Talend job is scored on the following dimensions (1–5 scale):

| Dimension | Weight | 1 (Low) | 3 (Medium) | 5 (High) |
|---|---|---|---|---|
| **Component Count** | 20% | 1–3 components | 4–8 components | 9+ components |
| **Transformation Complexity** | 25% | Direct copy | Filters + lookups | Multi-join tMap + aggregation |
| **Custom Code** | 20% | None | Simple expressions | Java routines / tJava |
| **Dependencies** | 15% | Standalone | 1–2 child jobs | 3+ child jobs / complex DAG |
| **Data Volume** | 10% | < 100K rows | 100K – 10M rows | > 10M rows |
| **Error Handling** | 10% | Basic | Conditional flows | Complex retry / compensation |

## Complexity Tiers

| Tier | Score Range | Migration Approach | Typical Effort |
|---|---|---|---|
| **Simple** | 1.0 – 2.0 | Direct translation, mostly automated | 2–4 hours |
| **Medium** | 2.1 – 3.5 | Semi-automated with manual review | 4–12 hours |
| **Complex** | 3.6 – 5.0 | Manual migration with redesign | 12–40 hours |

## Target Recommendation Rules

| Condition | Recommended Target |
|---|---|
| Score ≤ 2.0 and pattern = Copy | Data Factory Copy Activity |
| Score ≤ 3.0 and no custom code | Data Factory Data Flow |
| Score > 3.0 or has custom code | Spark Notebook |
| Pattern = Orchestration | Data Factory Pipeline |

## Sample Assessment

| Job | Components | Transforms | Custom Code | Dependencies | Volume | Error Handling | **Total** | **Tier** |
|---|---|---|---|---|---|---|---|---|
| sample_copy_customers | 1 | 1 | 1 | 1 | 2 | 1 | **1.15** | Simple |
| sample_transform_orders | 4 | 4 | 1 | 1 | 3 | 2 | **2.75** | Medium |
| sample_parent_daily | 2 | 1 | 1 | 5 | 1 | 3 | **2.05** | Medium |
| sample_file_load | 2 | 2 | 1 | 1 | 2 | 2 | **1.70** | Simple |
| sample_scd2_products | 4 | 5 | 5 | 1 | 3 | 3 | **3.85** | Complex |
