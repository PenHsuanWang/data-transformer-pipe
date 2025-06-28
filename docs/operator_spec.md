# ProcessPipe Operator Suite

Below is a reference design for the full 18 operators provided by **ProcessPipe**. Each section clarifies purpose, parameters, validation rules, and expected usage. Keep this document handy when developing or reviewing new operators.

\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

## 1  Common conventions

| Aspect | Guideline |
| ----- | --------- |
| Class base | Every concrete class extends `Operator` and implements `_execute_core`. |
| JSON schema | Keys follow the high level design (HLD) quick recap; optional fields are backwards compatible. |
| Validation | `pre_validate()` checks column presence, dtype compatibility, and parameter ranges. Failures raise an `XxxError`. |
| Logging | `INFO` logs start and finish with row/column counts. `DEBUG` dumps parameters and execution time. |
| Performance | All operators accept `copy: bool = False`. When `False`, return views where possible. Large-table ops expose `chunksize` or streaming variants where noted. |

\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

## 2  Operator blueprints

> ### 2.1 JoinOperator
>
> **Purpose**: relational join between two sources using equality or composite predicates.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `left` | str | yes | input table name |
> | `right` | str | yes | input table name |
> | `on` | list[[left,right]] | yes* | explicit column pairs |
> | `conditions` | list[str] | no | parallel comparison ops |
> | `how` | str | no | default `'inner'` |
> | `suffixes` | tuple[str,str] | no | duplicate column resolver |
> | `output` | str | no | defaults to `'join'` if omitted |
>
> **Validation & errors**
> * Ensure join columns exist in both frames.
> * Verify dtype compatibility, casting when safe. Mismatches raise `JoinError`.
> * If `validate` is provided, fail early when cardinality is violated.
>
> **Algorithm**
> 1. When `compare_ops` is absent, call `pd.merge` with provided args.
> 2. Otherwise perform a cartesian merge on all keys and filter rows with the compound predicate.
>
> **Performance notes**
> * Broadcast optimisation when one side is under 1M rows.
> * `sort=False` may be used to skip the merge sort step.
>
> **Example**
> ```json
> {"type": "join", "left": "orders", "right": "customers", "on": [["cust_id","cust_id"]], "conditions": ["eq"], "how": "left", "output": "orders_enriched"}
> ```
> DSL: `.join("orders", "customers", on=[("cust_id","cust_id")], how="left")`
>
> **Unit tests**
> * multi-key happy path
> * dtype mismatch → `CastError`
> * duplicate keys vs `validate='1:1'`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.2 UnionOperator
>
> **Purpose**: vertical concatenation of multiple tables.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `tables` | list[str] | yes | inputs to combine |
> | `ignore_index` | bool | no | default `False` |
> | `sort` | bool | no | default `False` |
> | `verify_schema` | bool | no | ensure identical column sets |
> | `output` | str | no | defaults to `'union'` |
>
> **Validation**
> * When `verify_schema=True`, fail if columns differ.
>
> **Algorithm**
> * Uses `pd.concat` along axis 0. Missing columns are filled with NaN unless `verify_schema` is true.
> * A `chunksize` parameter allows streaming concatenation for memory efficiency.
>
> **Example**
> ```json
> {"type": "union", "tables": ["north", "south"], "ignore_index": true}
> ```
> DSL: `.union("north", "south", ignore_index=True)`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.3 AggregationOperator
>
> **Purpose**: group-wise aggregation using `DataFrameGroupBy.agg`.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `groupby` | str or list[str] | yes | group keys |
> | `agg_map` | dict | yes | mapping of column → agg func |
> | `dropna` | bool | no | drop groups with NA keys |
> | `as_index` | bool | no | keep group keys as index |
> | `numeric_only` | bool | no | restrict to numeric columns |
> | `output` | str | no | defaults to `'aggregate'` |
>
> **Validation**
> * Ensure all columns in `agg_map` exist.
> * Reject unknown aggregation functions with `AggregationError`.
>
> **Algorithm**
> * Delegates to `df.groupby(groupby, dropna=dropna, as_index=as_index).agg(agg_map)`.
> * Supports callables; if they have attribute `engine='numba'`, apply JIT.
>
> **Example**
> ```json
> {"type": "aggregation", "input": "orders", "groupby": "cust_id", "agg_map": {"quantity": "sum"}}
> ```
> DSL: `.aggregate("orders", groupby="cust_id", agg_map={"quantity": "sum"})`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.4 GroupSizeOperator
>
> **Purpose**: annotate each row with the size of its group.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `groupby` | str or list[str] | yes | group keys |
> | `column_name` | str | no | default `'group_size'` |
> | `dtype` | str | no | default `'int32'` |
> | `output` | str | no | defaults to `'with_size'` |
>
> **Algorithm**
> * Uses `df.groupby(groupby).transform("size")` to compute counts.
> * Downcasts to `int32` when count < 2B.
>
> **Example**
> ```json
> {"type": "group_size", "input": "orders", "groupby": "cust_id"}
> ```
> DSL: `.group_size("orders", groupby="cust_id")`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.5 FilterOperator
>
> **Purpose**: SQL-like row filtering using `DataFrame.query`.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `condition` | str | yes | boolean expression |
> | `engine` | str | no | `'numexpr'` or `'python'` |
> | `safe_mode` | bool | no | force `'python'` engine |
> | `output` | str | no | defaults to `'filtered'` |
>
> **Validation**
> * Expression errors raise `FilterError`.
>
> **Example**
> ```json
> {"type": "filter", "input": "orders", "condition": "amount > 100"}
> ```
> DSL: `.filter("orders", condition="amount > 100")`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.6 RollingAggOperator
>
> **Purpose**: compute rolling-window aggregations.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `window` | int or str | yes | window size or offset |
> | `on` | str | no | column to order by |
> | `agg` | dict | yes | column → agg function |
> | `min_periods` | int | no | default `1` |
> | `center` | bool | no | default `False` |
> | `step` | int | no | compute every N rows |
> | `partition_by` | str or list[str] | no | apply per group |
> | `output` | str | no | defaults to `'rolling'` |
>
> **Algorithm**
> * Calls `df.rolling(window, on=on, min_periods=min_periods, center=center).agg(agg)`.
> * If `partition_by` is set, groups first then performs rolling per group.
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.7 SortOperator
>
> **Purpose**: sort rows by one or more columns.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `by` | str or list[str] | yes | columns to sort |
> | `ascending` | bool or list[bool] | no | default `True` |
> | `na_position` | str | no | `'last'` or `'first'` |
> | `kind` | str | no | `'mergesort'`, `'quicksort'`, etc. |
> | `output` | str | no | defaults to `'sorted'` |
>
> **Example**
> ```json
> {"type": "sort", "input": "orders", "by": ["date", "amount"], "ascending": [true, false]}
> ```
> DSL: `.sort("orders", by=["date", "amount"], ascending=[True, False])`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.8 TopNOperator
>
> **Purpose**: return the top (or bottom) N rows by metric.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `n` | int | yes | number of rows |
> | `metric` | str | yes | column to rank |
> | `largest` | bool | no | choose nlargest or nsmallest |
> | `per_group` | bool | no | compute per group |
> | `group_keys` | str or list[str] | no | group keys when `per_group=True` |
> | `output` | str | no | defaults to `'topn'` |
>
> **Example**
> ```json
> {"type": "topn", "input": "orders", "n": 10, "metric": "amount", "largest": true}
> ```
> DSL: `.topn("orders", n=10, metric="amount")`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.9 FillNAOperator
>
> **Purpose**: fill null values via scalar value or method.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `value` | scalar | no | fill with this value |
> | `method` | str | no | `'ffill'`/`'bfill'` |
> | `limit` | int | no | max consecutive fills |
> | `groupby` | str or list[str] | no | group-aware fill |
> | `output` | str | no | defaults to `'filled'` |
>
> **Example**
> ```json
> {"type": "fillna", "input": "orders", "value": 0}
> ```
> DSL: `.fillna("orders", value=0)`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.10 RenameOperator
>
> **Purpose**: rename columns or index labels.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `columns` | dict | no | old → new column names |
> | `index` | dict | no | old → new index names |
> | `level` | int or str | no | multi-index level |
> | `errors` | str | no | `'ignore'` or `'raise'` |
> | `output` | str | no | defaults to `'renamed'` |
>
> **Example**
> ```json
> {"type": "rename", "input": "orders", "columns": {"old": "new"}}
> ```
> DSL: `.rename("orders", columns={"old": "new"})`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.11 CastOperator
>
> **Purpose**: change the dtype of columns.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `dtype_map` | dict | yes | column → dtype |
> | `errors` | str | no | `'raise'` or `'ignore'` |
> | `downcast` | str | no | `'integer'`, `'signed'`, `'unsigned'`, `'float'` |
> | `output` | str | no | defaults to `'casted'` |
>
> **Example**
> ```json
> {"type": "cast", "input": "orders", "dtype_map": {"qty": "int32"}}
> ```
> DSL: `.cast("orders", dtype_map={"qty": "int32"})`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.12 StringOperator
>
> **Purpose**: apply vectorised string operations.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `column` | str | yes | target column |
> | `op` | str | yes | e.g., `'contains'`, `'replace'` |
> | `pattern` | str | no | pattern for operation |
> | `replacement` | str | no | used by `'replace'` |
> | `regex` | bool | no | treat pattern as regex |
> | `case` | bool | no | case sensitivity |
> | `flags` | int | no | regex flags |
> | `output` | str | no | defaults to `'string_op'` |
>
> **Example**
> ```json
> {"type": "string", "input": "orders", "column": "name", "op": "contains", "pattern": "foo"}
> ```
> DSL: `.string("orders", column="name", op="contains", pattern="foo")`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.13 DropDuplicateOperator
>
> **Purpose**: remove duplicate rows.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `subset` | str or list[str] | no | columns to consider |
> | `keep` | str | no | `'first'`, `'last'`, `'false'` |
> | `ignore_index` | bool | no | default `False` |
> | `tolerance` | float | no | rounding tolerance |
> | `output` | str | no | defaults to `'deduped'` |
>
> **Example**
> ```json
> {"type": "drop_duplicate", "input": "orders", "subset": "id"}
> ```
> DSL: `.drop_duplicates("orders", subset="id")`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.14 PartitionAggOperator
>
> **Purpose**: apply aggregations within partitions while preserving row count.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `groupby` | str or list[str] | yes | partition keys |
> | `agg_map` | dict | yes | column → agg func |
> | `sort_by` | str or list[str] | no | ordering columns |
> | `output` | str | no | defaults to `'partition_agg'` |
>
> **Example**
> ```json
> {"type": "partition_agg", "input": "orders", "groupby": "cust_id", "agg_map": {"quantity": "sum"}}
> ```
> DSL: `.partition_agg("orders", groupby="cust_id", agg_map={"quantity": "sum"})`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.15 RowNumberOperator
>
> **Purpose**: assign a row number per partition.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `order_by` | str or list[str] | no | deterministic ordering |
> | `partition_by` | str or list[str] | no | partitioning keys |
> | `output` | str | no | defaults to `'with_row_number'` |
>
> **Example**
> ```json
> {"type": "row_number", "input": "orders", "partition_by": "cust_id"}
> ```
> DSL: `.row_number("orders", partition_by="cust_id")`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.16 DeleteOperator
>
> **Purpose**: remove rows matching a boolean condition.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `condition` | str | yes | boolean expression |
> | `output` | str | no | defaults to `'deleted'` |
>
> **Result**
> * Returns the filtered DataFrame and meta information `{"rows_removed": int}`.
>
> **Example**
> ```json
> {"type": "delete", "input": "orders", "condition": "status == 'cancelled'"}
> ```
> DSL: `.delete("orders", condition="status == 'cancelled'")`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.17 UpdateOperator
>
> **Purpose**: update column values conditionally.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `condition` | str | yes | boolean expression |
> | `set` | dict | yes | column → value or Series |
> | `broadcast` | bool | no | align set values by index |
> | `output` | str | no | defaults to `'updated'` |
>
> **Example**
> ```json
> {"type": "update", "input": "orders", "condition": "amount < 0", "set": {"amount": 0}}
> ```
> DSL: `.update("orders", condition="amount < 0", set={"amount": 0})`
>
> \u2500\u2500\u2500\u2500\n>
> ### 2.18 CaseOperator
>
> **Purpose**: apply vectorised multi-branch logic.
>
> **Parameters**
>
> | name | type | required | notes |
> | ---- | ---- | -------- | ----- |
> | `input` | str | yes | source table |
> | `conditions` | list[str] | yes | boolean expressions |
> | `choices` | list | yes | values aligned with conditions |
> | `default` | any | no | value when no condition matches |
> | `output` | str | no | new column name |
>
> **Algorithm**
> * Use `numpy.select` to build the new column.
> * Downgrade to `numpy.where` when a single condition is provided.
>
> **Example**
> ```json
> {"type": "case", "input": "orders", "conditions": ["amount > 100"], "choices": ["big"], "default": "small", "output": "size"}
> ```
> DSL: `.case("orders", conditions=["amount > 100"], choices=["big"], default="small", output="size")`
>
> \u2500\u2500\u2500\u2500\n>
> ## 3  Testing & coverage matrix
>
> Each operator is covered by unit tests for both happy-path and edge cases. See `tests/matrix.xlsx` for the full matrix.
>
> | Operator | Happy-path cases | Edge cases | Perf benchmark |
> | -------- | ---------------- | ---------- | -------------- |
> | Join | ✓ | duplicate keys, dtype mismatch | 10M rows vs 10k rows |
> | Union | ✓ | column mismatch | 100× 1M row shards |
> | ... | ... | ... | ... |
>
> \u2500\u2500\u2500\u2500\n>
> ## 4  Change-control reminder
>
> Any new operator must update the following:
>
> 1. `processpipe/processpipe_pkg/operators/__init__.py` export list
> 2. `ProcessPipe.build_pipe()` keyword map
> 3. JSON schema enum of valid `type` values
> 4. The operator table in this document
>
> Keeping these in sync ensures that declarative plans work correctly and that new features remain discoverable.
>
