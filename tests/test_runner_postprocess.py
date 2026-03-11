from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runner import (
    collapse_equivalent_partition_writes,
    convert_updates_to_lineage_inserts,
    _canonicalize_temp_assets,
    _contract_score,
    add_partition_lineage_surrogates,
    add_transitive_temp_target_surrogates,
    apply_prompt_compliance_pass,
    augment_missing_write_flows_from_ir,
    build_known_columns_from_temp_sources,
    build_recovery_sql_from_ir,
    enforce_known_columns,
    extract_known_temp_columns,
    fix_missing_subquery_closing_parenthesis,
    fix_missing_join_wrapper_parentheses,
    fix_missing_window_by_spacing,
    fix_missing_rownum_wrapper_parentheses,
    normalize_tsql_placeholders,
    keep_insert_select_statements,
    normalize_partition_clauses,
    remove_metric_and_noop_self_writes,
    remove_where_on_temp_source_inserts,
    rewrite_temp_hop_writes_to_direct_source,
    strip_partition_clause_in_inserts,
    normalize_and_dedupe_statements,
    reorder_statements_by_ir,
    substitute_known_variables,
    enforce_asset_case_from_ir,
    enforce_column_case_from_ir,
    drop_syntax_invalid_write_statements,
    uniquify_reused_subquery_aliases,
)
from src.ir.models import CodeSpan, LineageIR, LineageOp


class RunnerPostprocessTest(unittest.TestCase):
    def test_substitute_known_variables(self) -> None:
        sql = "INSERT INTO t SELECT * FROM {STAGING_TABLE};"
        out = substitute_known_variables(sql, {"STAGING_TABLE": "db.stage"})
        self.assertIn("db.stage", out)
        self.assertNotIn("{STAGING_TABLE}", out)

    def test_enforce_asset_case_from_ir(self) -> None:
        ir = LineageIR(
            file_path="dummy.py",
            operations=[
                LineageOp(
                    op_id="1",
                    op_type="write",
                    code_span=CodeSpan(1, 1),
                    raw_line="",
                    source_assets=["PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP"],
                    target_assets=["PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD"],
                )
            ],
            variables={
                "TARGET_TABLE": "PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD",
                "STAGING_TABLE": "PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP",
            },
        )
        sql = (
            "insert into prd_acl_datalake.mart_customer_icons_scd_prod "
            "select * from prd_acl_datalake.mart_customer_icons_temp;"
        )
        out = enforce_asset_case_from_ir(sql, ir)
        self.assertIn("PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD", out)
        self.assertIn("PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP", out)

    def test_enforce_column_case_from_ir(self) -> None:
        ir = LineageIR(
            file_path="dummy.py",
            operations=[
                LineageOp(
                    op_id="1",
                    op_type="write",
                    code_span=CodeSpan(1, 1),
                    raw_line="",
                    source_assets=["PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP"],
                    target_assets=["PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD"],
                    metadata={
                        "sql_text": (
                            "INSERT INTO PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD "
                            "SELECT CIF_KEY, END_DATE, START_DATE "
                            "FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP"
                        )
                    },
                )
            ],
        )
        sql = (
            "insert into PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD "
            "select cif_key, end_date, start_date "
            "from PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP;"
        )
        out = enforce_column_case_from_ir(sql, ir)
        self.assertIn("select CIF_KEY, END_DATE, START_DATE", out)

    def test_drop_syntax_invalid_write_statements(self) -> None:
        sql = (
            "INSERT INTO tgt SELECT a, b FROM src;\n"
            "INSERT INTO tgt SELECT a, b, ;\n"
            "UPDATE tgt SET a = a FROM src;"
        )
        out = drop_syntax_invalid_write_statements(sql)
        self.assertIn("INSERT INTO tgt SELECT a, b FROM src;", out)
        self.assertIn("UPDATE tgt SET a = a FROM src;", out)
        self.assertNotIn("SELECT a, b, ;", out)

    def test_uniquify_reused_subquery_aliases(self) -> None:
        sql = (
            "INSERT INTO t "
            "SELECT CHANNEL.* FROM (SELECT CHANNEL.*, ROW_NUMBER() OVER(PARTITION BY id ORDER BY dt DESC) RN "
            "FROM (SELECT id, dt FROM src) AS CHANNEL) AS CHANNEL WHERE RN = 1) AS CHANNEL ON 1=1;"
        )
        out = uniquify_reused_subquery_aliases(sql)
        self.assertIn(") AS CHANNEL_1", out)
        self.assertIn(") AS CHANNEL_2", out)
        self.assertIn(") AS CHANNEL ON", out)
        self.assertIn("SELECT CHANNEL_1.*", out)

    def test_fix_missing_rownum_wrapper_parentheses_noop(self) -> None:
        sql = (
            "INSERT INTO t SELECT * FROM src "
            "LEFT JOIN (SELECT * FROM (SELECT * FROM x) AS CHANNEL ) AS CHANNEL "
            "WHERE RN = 1 ) AS CHANNEL ON src.id = CHANNEL.id;"
        )
        out = fix_missing_rownum_wrapper_parentheses(sql)
        self.assertEqual(out, sql)

    def test_enforce_known_columns(self) -> None:
        sql = "INSERT INTO target_tbl SELECT * FROM src_tbl;"
        out = enforce_known_columns(sql, {"target_tbl": ["a", "b", "c"]})
        self.assertIn("SELECT a, b, c FROM", out)

    def test_enforce_known_columns_reorders_explicit_select(self) -> None:
        sql = "INSERT INTO target_tbl SELECT c, a, b FROM src_tbl;"
        out = enforce_known_columns(sql, {"target_tbl": ["a", "b", "c"]})
        self.assertIn("SELECT a, b, c FROM", out)

    def test_enforce_known_columns_uses_source_when_target_unknown(self) -> None:
        sql = "INSERT INTO target_tbl SELECT * FROM temp_table;"
        out = enforce_known_columns(sql, {"temp_table": ["a", "b", "c"]})
        self.assertIn("SELECT a, b, c FROM", out)

    def test_enforce_known_columns_keeps_self_update_surrogate_projection(self) -> None:
        sql = (
            "INSERT INTO target_tbl "
            "SELECT tgt.c1, tgt.c2, tgt.c3 "
            "FROM target_tbl AS tgt JOIN stg_tbl AS src1 ON 1=1;"
        )
        out = enforce_known_columns(sql, {"target_tbl": ["x1", "x2", "x3", "x4"]})
        self.assertIn("SELECT tgt.c1, tgt.c2, tgt.c3", out)
        self.assertNotIn("SELECT x1, x2, x3, x4", out)

    def test_enforce_known_columns_allows_expressions_with_from_keyword(self) -> None:
        sql = "INSERT INTO target_tbl SELECT * FROM src_tbl;"
        known = {"target_tbl": ["EXTRACT(DAY FROM dt) AS day_num", "id"]}
        out = enforce_known_columns(sql, known)
        self.assertIn("EXTRACT(DAY FROM dt) AS day_num", out)

    def test_normalize_and_dedupe(self) -> None:
        sql = "INSERT INTO t SELECT * FROM s;\nINSERT INTO t   SELECT * FROM s;"
        out = normalize_and_dedupe_statements(sql)
        self.assertEqual(out.count("INSERT INTO t SELECT * FROM s;"), 1)

    def test_strip_non_sql_prefix(self) -> None:
        noisy = "Explanation text. INSERT INTO t SELECT * FROM s;"
        out = keep_insert_select_statements(noisy)
        self.assertEqual(out, "INSERT INTO t SELECT * FROM s;")

    def test_keep_update_statement(self) -> None:
        noisy = "Plan: UPDATE tgt SET col = col FROM src;"
        out = keep_insert_select_statements(noisy)
        self.assertEqual(out, "UPDATE tgt SET col = col FROM src;")

    def test_convert_update_to_lineage_insert(self) -> None:
        sql = (
            "UPDATE PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD "
            "SET END_DATE = DATE_ADD('{TANGGAL_POSISI}',-1) "
            "WHERE CIF_KEY IN (SELECT CIF_KEY FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP WHERE label='UPDATED') "
            "AND DATE_ADD('{TANGGAL_POSISI}',-1) BETWEEN START_DATE AND END_DATE;"
        )
        known = {
            "prd_acl_datalake.mart_customer_icons_scd_prod": [
                "cif_key",
                "end_date",
                "start_date",
            ]
        }
        out = convert_updates_to_lineage_inserts(sql, known)
        self.assertTrue(out.strip().upper().startswith("INSERT INTO PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD"))
        self.assertIn("select tgt.cif_key, tgt.end_date, tgt.start_date", out.lower())
        self.assertIn("join prd_acl_datalake.mart_customer_icons_temp as src1 on 1=1", out.lower())

    def test_remove_metric_self_copy_insert(self) -> None:
        sql = (
            "INSERT INTO PRD_ACL_DATALAKE.MASTER_CTA SELECT * FROM PRD_ACL_DATALAKE.MASTER_CTA;\n"
            "INSERT INTO target_tbl SELECT a, b FROM src_tbl;"
        )
        out = remove_metric_and_noop_self_writes(sql)
        self.assertNotIn("MASTER_CTA SELECT * FROM PRD_ACL_DATALAKE.MASTER_CTA", out)
        self.assertIn("INSERT INTO target_tbl SELECT a, b FROM src_tbl;", out)

    def test_recovery_from_ir(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            variables={"TARGET_TABLE": "PRD_ACL_DATALAKE.MASTER_CTA"},
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(1, 1),
                    raw_line="hwc.executeUpdate(sc_insert_query)",
                    source_assets=["PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP"],
                    target_assets=["PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD"],
                )
            ],
        )
        out = build_recovery_sql_from_ir(ir, {})
        self.assertIn("INSERT INTO PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_SCD_PROD", out)
        self.assertIn("FROM PRD_ACL_DATALAKE.MART_CUSTOMER_ICONS_TEMP", out)

    def test_recovery_joins_multiple_sources(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(1, 1),
                    raw_line="spark.sql(write_query)",
                    source_assets=["src_a", "src_b"],
                    target_assets=["target_tbl"],
                )
            ],
        )
        out = build_recovery_sql_from_ir(ir, {})
        self.assertIn("FROM src_a, src_b", out)

    def test_temp_columns_propagate_to_target(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="read",
                    code_span=CodeSpan(1, 1),
                    raw_line="spark.sql(read)",
                    source_assets=["src_tbl"],
                    metadata={"sql_text": "SELECT a AS col_a, b AS col_b FROM src_tbl"},
                ),
                LineageOp(
                    op_id="op_2",
                    op_type="write",
                    code_span=CodeSpan(2, 2),
                    raw_line="df.createOrReplaceTempView('temp_table')",
                    target_assets=["#temp_table"],
                ),
                LineageOp(
                    op_id="op_3",
                    op_type="write",
                    code_span=CodeSpan(3, 3),
                    raw_line="spark.sql(write_query)",
                    source_assets=["temp_table"],
                    target_assets=["target_tbl"],
                    metadata={"sql_text": "INSERT INTO target_tbl SELECT * FROM temp_table"},
                ),
            ],
            allowed_temp_assets={"#temp_table"},
        )
        temp_cols = extract_known_temp_columns(ir)
        known = build_known_columns_from_temp_sources(ir, temp_cols)
        out = enforce_known_columns("INSERT INTO target_tbl SELECT * FROM temp_table;", known)
        self.assertIn("SELECT col_a, col_b FROM", out)
        out_temp = enforce_known_columns("INSERT INTO temp_table SELECT * FROM src_tbl;", temp_cols)
        self.assertIn("SELECT col_a, col_b FROM", out_temp)

    def test_partitioned_target_key_still_enforces_columns(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            variables={"TARGET_TABLE": "PRD_ACL_DATALAKE.MASTER_CTA"},
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="read",
                    code_span=CodeSpan(1, 1),
                    raw_line="spark.sql(read)",
                    source_assets=["src_tbl"],
                    metadata={"sql_text": "SELECT a, b FROM src_tbl"},
                ),
                LineageOp(
                    op_id="op_2",
                    op_type="write",
                    code_span=CodeSpan(2, 2),
                    raw_line="df.createOrReplaceTempView('temp_table')",
                    target_assets=["#temp_table"],
                ),
                LineageOp(
                    op_id="op_3",
                    op_type="write",
                    code_span=CodeSpan(3, 3),
                    raw_line="spark.sql(write_query)",
                    source_assets=["temp_table"],
                    target_assets=["TARGET_TABLE PARTITION(as_of_date)"],
                    metadata={"sql_text": "INSERT INTO {TARGET_TABLE} PARTITION (as_of_date) SELECT * FROM temp_table"},
                ),
            ],
            allowed_temp_assets={"#temp_table"},
        )
        temp_cols = extract_known_temp_columns(ir)
        known = build_known_columns_from_temp_sources(ir, temp_cols)
        out = enforce_known_columns(
            "INSERT INTO PRD_ACL_DATALAKE.MASTER_CTA PARTITION(as_of_date) SELECT * FROM temp_table;",
            known,
        )
        self.assertIn("SELECT a, b FROM", out)
        self.assertIn("INSERT INTO PRD_ACL_DATALAKE.MASTER_CTA PARTITION(as_of_date)", out)

    def test_reorder_by_ir_execution_order(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(1, 1),
                    raw_line="temp write",
                    target_assets=["#temp_table"],
                    metadata={"sql_text": "INSERT INTO temp_table SELECT * FROM src_tbl"},
                ),
                LineageOp(
                    op_id="op_2",
                    op_type="write",
                    code_span=CodeSpan(2, 2),
                    raw_line="final write",
                    target_assets=["target_tbl"],
                    metadata={"sql_text": "INSERT INTO target_tbl SELECT * FROM temp_table"},
                ),
            ],
            allowed_temp_assets={"#temp_table"},
        )
        sql = "INSERT INTO target_tbl SELECT * FROM temp_table;\nINSERT INTO temp_table SELECT * FROM src_tbl;"
        out = reorder_statements_by_ir(sql, ir)
        self.assertTrue(out.splitlines()[0].startswith("INSERT INTO temp_table"))

    def test_normalize_duplicate_partition_clauses(self) -> None:
        sql = "INSERT INTO TARGET_TABLE PARTITION(as_of_date) PARTITION (as_of_date) SELECT * FROM temp_table;"
        out = normalize_partition_clauses(sql)
        self.assertEqual(out.count("PARTITION"), 1)

    def test_recovery_uses_evidence_join_predicate(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="read",
                    code_span=CodeSpan(1, 1),
                    raw_line="spark.sql(read_query)",
                    source_assets=["src_a", "src_b"],
                    metadata={
                        "sql_text": "SELECT a.id AS id, b.name AS name FROM src_a a JOIN src_b b ON a.id = b.id",
                        "statement_kind": "select",
                    },
                ),
                LineageOp(
                    op_id="op_2",
                    op_type="write",
                    code_span=CodeSpan(2, 2),
                    raw_line="df.createOrReplaceTempView('temp_table')",
                    target_assets=["#temp_table"],
                    metadata={"statement_kind": "create_temp_view"},
                ),
            ],
            allowed_temp_assets={"#temp_table"},
        )
        out = build_recovery_sql_from_ir(ir, {})
        self.assertIn("JOIN src_b b ON a.id = b.id", out)
        self.assertNotIn("ON 1=1", out)

    def test_expression_projection_is_preserved(self) -> None:
        sql = "INSERT INTO target_tbl SELECT x, y FROM src_tbl;"
        known = {"target_tbl": ["CASE WHEN x > 0 THEN x ELSE 0 END AS x_norm", "COALESCE(y, 0) AS y_norm"]}
        out = enforce_known_columns(sql, known)
        self.assertIn("CASE WHEN x > 0 THEN x ELSE 0 END AS x_norm", out)
        self.assertIn("COALESCE(y, 0) AS y_norm", out)

    def test_augment_missing_write_flows(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="read",
                    code_span=CodeSpan(1, 1),
                    raw_line="spark.sql(read)",
                    source_assets=["src_tbl"],
                ),
                LineageOp(
                    op_id="op_2",
                    op_type="write",
                    code_span=CodeSpan(2, 2),
                    raw_line="df.createOrReplaceTempView('temp_table')",
                    target_assets=["#temp_table"],
                ),
                LineageOp(
                    op_id="op_3",
                    op_type="write",
                    code_span=CodeSpan(3, 3),
                    raw_line="spark.sql(write_query)",
                    source_assets=["temp_table"],
                    target_assets=["target_tbl"],
                ),
            ],
            allowed_temp_assets={"#temp_table"},
        )
        out = augment_missing_write_flows_from_ir(
            "INSERT INTO target_tbl SELECT * FROM #temp_table;",
            ir,
            {},
        )
        self.assertIn("INSERT INTO #temp_table SELECT * FROM src_tbl;", out)

    def test_enforce_columns_after_augment(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="read",
                    code_span=CodeSpan(1, 1),
                    raw_line="spark.sql(read)",
                    source_assets=["src_tbl"],
                    metadata={"sql_text": "SELECT a, b FROM src_tbl"},
                ),
                LineageOp(
                    op_id="op_2",
                    op_type="write",
                    code_span=CodeSpan(2, 2),
                    raw_line="df.createOrReplaceTempView('temp_table')",
                    target_assets=["#temp_table"],
                ),
                LineageOp(
                    op_id="op_3",
                    op_type="write",
                    code_span=CodeSpan(3, 3),
                    raw_line="spark.sql(write_query)",
                    source_assets=["temp_table"],
                    target_assets=["target_tbl"],
                    metadata={"sql_text": "INSERT INTO target_tbl SELECT * FROM temp_table"},
                ),
            ],
            allowed_temp_assets={"#temp_table"},
        )
        temp_cols = extract_known_temp_columns(ir)
        known = build_known_columns_from_temp_sources(ir, temp_cols)
        augmented = augment_missing_write_flows_from_ir("INSERT INTO temp_table SELECT a, b FROM src_tbl;", ir, known)
        out = enforce_known_columns(augmented, known)
        self.assertIn("INSERT INTO target_tbl SELECT a, b FROM temp_table;", out)

    def test_contract_score_prefers_complete_sources(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(1, 1),
                    raw_line="spark.sql(write)",
                    source_assets=["src_a", "src_b"],
                    target_assets=["target_tbl"],
                    metadata={"statement_kind": "insert"},
                )
            ],
        )
        low = _contract_score("INSERT INTO target_tbl SELECT * FROM src_a;", ir)
        high = _contract_score("INSERT INTO target_tbl SELECT * FROM src_a JOIN src_b ON src_a.id = src_b.id;", ir)
        self.assertGreater(high, low)

    def test_canonicalize_temp_assets_renders_without_hash(self) -> None:
        ir = LineageIR(file_path="x.py", allowed_temp_assets={"#temp_table"})
        out = _canonicalize_temp_assets(
            "INSERT INTO #temp_table SELECT * FROM src; INSERT INTO target SELECT * FROM #temp_table;",
            ir,
        )
        self.assertIn("#temp_table", out)
        self.assertIn("INSERT INTO #temp_table", out)

    def test_normalize_and_dedupe_prefers_explicit_temp_insert(self) -> None:
        ir = LineageIR(file_path="x.py", allowed_temp_assets={"#temp_table"})
        sql = (
            "INSERT INTO #temp_table SELECT * FROM src_tbl;\n"
            "INSERT INTO temp_table SELECT a, b FROM src_tbl;\n"
            "INSERT INTO target_tbl SELECT * FROM temp_table;"
        )
        out = normalize_and_dedupe_statements(sql, ir)
        self.assertEqual(out.upper().count("INSERT INTO #TEMP_TABLE"), 1)
        self.assertIn("SELECT a, b FROM src_tbl", out)

    def test_normalize_and_dedupe_keeps_multiple_temp_targets(self) -> None:
        ir = LineageIR(file_path="x.py", allowed_temp_assets={"#temp_a", "#temp_b"})
        sql = (
            "INSERT INTO #temp_a SELECT a FROM src1;\n"
            "INSERT INTO #temp_b SELECT b FROM src2;\n"
            "INSERT INTO target_tbl SELECT * FROM temp_b;"
        )
        out = normalize_and_dedupe_statements(sql, ir)
        self.assertEqual(out.lower().count("insert into #temp_a"), 1)
        self.assertEqual(out.lower().count("insert into #temp_b"), 1)

    def test_canonicalize_temp_assets_does_not_collapse_similar_names(self) -> None:
        ir = LineageIR(file_path="x.py", allowed_temp_assets={"#temp_table", "#temp_table_1"})
        sql = (
            "INSERT INTO #temp_table SELECT a FROM src;\n"
            "INSERT INTO #temp_table_1 SELECT b FROM src;"
        )
        out = normalize_and_dedupe_statements(sql, ir)
        self.assertIn("INSERT INTO #temp_table SELECT a FROM src;", out)
        self.assertIn("INSERT INTO #temp_table_1 SELECT b FROM src;", out)

    def test_prompt_compliance_pass_removes_where(self) -> None:
        ir = LineageIR(file_path="x.py")
        sql = "INSERT INTO target_tbl SELECT a, b FROM src_tbl WHERE a > 10;"
        out = apply_prompt_compliance_pass(
            sql_text=sql,
            ir=ir,
            known_columns_by_target={},
            no_where=True,
        )
        self.assertNotIn(" WHERE ", out.upper())

    def test_prompt_compliance_pass_baseline_profile_keeps_where(self) -> None:
        ir = LineageIR(file_path="x.py")
        sql = "INSERT INTO target_tbl SELECT a, b FROM src_tbl WHERE a > 10;"
        out = apply_prompt_compliance_pass(
            sql_text=sql,
            ir=ir,
            known_columns_by_target={},
            no_where=False,
            write_only=False,
        )
        self.assertIn(" WHERE ", out.upper())

    def test_prompt_compliance_pass_strips_comments(self) -> None:
        ir = LineageIR(file_path="x.py")
        sql = "INSERT INTO tgt SELECT a FROM src -- inline comment\nWHERE a > 1;"
        out = apply_prompt_compliance_pass(
            sql_text=sql,
            ir=ir,
            known_columns_by_target={},
            no_where=False,
            write_only=False,
        )
        self.assertNotIn("--", out)

    def test_add_partition_lineage_surrogate(self) -> None:
        sql = "INSERT INTO tgt PARTITION(as_of_date) SELECT a, b FROM #temp;"
        out = add_partition_lineage_surrogates(sql)
        self.assertIn("INSERT INTO tgt SELECT a, b FROM #temp;", out)

    def test_collapse_equivalent_partition_writes_prefers_single_plain(self) -> None:
        sql = (
            "INSERT INTO tgt PARTITION(as_of_date) SELECT a, b FROM #temp;\n"
            "INSERT INTO tgt SELECT a, b FROM #temp;"
        )
        out = collapse_equivalent_partition_writes(sql)
        self.assertEqual(out.upper().count("INSERT INTO TGT"), 1)
        self.assertIn("INSERT INTO tgt SELECT a, b FROM #temp;", out)

    def test_add_transitive_temp_target_surrogate(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(1, 1),
                    raw_line="temp write",
                    target_assets=["#temp_table"],
                ),
                LineageOp(
                    op_id="op_2",
                    op_type="write",
                    code_span=CodeSpan(2, 2),
                    raw_line="final write",
                    source_assets=["#temp_table"],
                    target_assets=["target_tbl"],
                ),
            ],
            allowed_temp_assets={"#temp_table"},
        )
        sql = "INSERT INTO #temp_table SELECT a, b FROM src_tbl;"
        out = add_transitive_temp_target_surrogates(sql, ir)
        self.assertIn("INSERT INTO target_tbl SELECT a, b FROM src_tbl;", out)

    def test_remove_where_on_temp_source_insert(self) -> None:
        sql = "INSERT INTO tgt PARTITION(as_of_date) SELECT a FROM #temp WHERE x = 1;"
        out = remove_where_on_temp_source_inserts(sql)
        self.assertEqual(out.strip(), "INSERT INTO tgt PARTITION(as_of_date) SELECT a FROM #temp;")

    def test_strip_partition_clause_in_inserts(self) -> None:
        sql = "INSERT INTO tgt PARTITION(as_of_date) SELECT a, b FROM src;"
        out = strip_partition_clause_in_inserts(sql)
        self.assertEqual(out.strip(), "INSERT INTO tgt SELECT a, b FROM src;")

    def test_rewrite_temp_hop_writes_to_direct_source(self) -> None:
        ir = LineageIR(
            file_path="x.py",
            operations=[
                LineageOp(
                    op_id="op_1",
                    op_type="write",
                    code_span=CodeSpan(1, 1),
                    raw_line="temp write",
                    target_assets=["#temp_table"],
                ),
                LineageOp(
                    op_id="op_2",
                    op_type="write",
                    code_span=CodeSpan(2, 2),
                    raw_line="final write",
                    source_assets=["#temp_table"],
                    target_assets=["target_tbl"],
                ),
            ],
            allowed_temp_assets={"#temp_table"},
        )
        sql = (
            "INSERT INTO #temp_table SELECT a, b FROM src_tbl;\n"
            "INSERT INTO target_tbl SELECT a, b FROM #temp_table;"
        )
        out = rewrite_temp_hop_writes_to_direct_source(sql, ir)
        self.assertIn("INSERT INTO target_tbl SELECT a, b FROM src_tbl;", out)
        self.assertNotIn("FROM #temp_table", out.splitlines()[-1])

    def test_fix_missing_rownum_wrapper_parentheses(self) -> None:
        sql = (
            "INSERT INTO tgt SELECT x FROM src_a a "
            "LEFT JOIN ( SELECT * FROM ( SELECT b.* FROM src_b b ) AS CHANNEL "
            "WHERE RN = 1 ) AS CHANNEL ON a.id = CHANNEL.id;"
        )
        out = fix_missing_rownum_wrapper_parentheses(sql)
        self.assertIn(") AS CHANNEL ) AS CHANNEL WHERE RN = 1 ) AS CHANNEL ON", out)
        out2 = fix_missing_rownum_wrapper_parentheses(out)
        self.assertEqual(out2, out)

    def test_fix_missing_window_by_spacing(self) -> None:
        sql = "SELECT ROW_NUMBER() OVER(PARTITION BYID_NUMBER ORDER BY AS_OF_DATE DESC) rn FROM t;"
        out = fix_missing_window_by_spacing(sql)
        self.assertIn("PARTITION BY ID_NUMBER", out)

    def test_fix_missing_subquery_closing_parenthesis(self) -> None:
        sql = "INSERT INTO #temp_table SELECT a FROM (SELECT a FROM src_a UNION ALL SELECT a FROM src_b JT;"
        out = fix_missing_subquery_closing_parenthesis(sql)
        self.assertIn("FROM src_b) JT;", out)

    def test_fix_missing_join_wrapper_parentheses(self) -> None:
        sql = (
            "INSERT INTO #temp_table SELECT x FROM t "
            "LEFT JOIN ( SELECT * FROM ( SELECT * FROM s ) AS CHANNEL ON t.id = CHANNEL.id;"
        )
        out = fix_missing_join_wrapper_parentheses(sql)
        self.assertIn(") AS CHANNEL ) AS CHANNEL ON", out)

    def test_normalize_tsql_placeholders(self) -> None:
        sql = "SELECT * FROM t WHERE as_of_date = '{TANGGAL_POSISI}' OR d = {TANGGAL_POSISI};"
        out = normalize_tsql_placeholders(sql)
        self.assertIn("as_of_date = @TANGGAL_POSISI", out)
        self.assertIn("d = @TANGGAL_POSISI", out)
        self.assertNotIn("{TANGGAL_POSISI}", out)

    def test_apply_prompt_compliance_pass_baseline_sql_parity_preserves_shape(self) -> None:
        ir = LineageIR(file_path="x.py")
        sql = "WITH df AS (SELECT * FROM src) INSERT INTO tgt SELECT * FROM df;"
        out = apply_prompt_compliance_pass(
            sql,
            ir,
            known_columns_by_target={"tgt": ["a"]},
            no_where=False,
            compliance_profile="baseline_sql_parity",
            write_only=True,
        )
        self.assertEqual(out.strip(), sql)


if __name__ == "__main__":
    unittest.main()

