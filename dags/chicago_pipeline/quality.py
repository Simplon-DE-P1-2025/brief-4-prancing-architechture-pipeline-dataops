import os
from datetime import datetime

import pandas as pd
import yaml
from soda_core.contracts import verify_contract_locally

from dags.chicago_pipeline.constants import (
    CLEAN_CSV_PATH,
    PROCESSED_CONTRACT_PATH,
    PROCESSED_CONTRACT_TABLE,
    PROCESSED_QUARANTINE_CSV_PATH,
    PROCESSED_QUARANTINE_TABLE,
    PROCESSED_VALID_CSV_PATH,
    PROCESSED_VALID_TABLE,
    RAW_CONTRACT_PATH,
    RAW_CONTRACT_TABLE,
    RAW_CSV_PATH,
    RAW_EXTRACTED_CSV_PATH,
    RAW_QUARANTINE_CSV_PATH,
    RAW_QUARANTINE_TABLE,
    REPORTS_DIR,
    SODA_CONFIG_PATH,
)
from dags.chicago_pipeline.database import replace_table_from_dataframe


def load_contract_definition(contract_path: str) -> dict:
    with open(contract_path, encoding="utf-8") as contract_file:
        return yaml.safe_load(contract_file)


def append_reason(reason_map: pd.Series, mask: pd.Series, reason: str) -> pd.Series:
    if not mask.any():
        return reason_map
    existing = reason_map.loc[mask].fillna("")
    separator = existing.ne("").map({True: "|", False: ""})
    reason_map.loc[mask] = existing + separator + reason
    return reason_map


def build_quarantine_from_contract(
    df: pd.DataFrame, contract_path: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    contract = load_contract_definition(contract_path)
    quarantine_mask = pd.Series(False, index=df.index)
    quarantine_reasons = pd.Series("", index=df.index, dtype="object")

    for column_definition in contract.get("columns", []):
        column_name = column_definition.get("name")
        if not column_name or column_name not in df.columns:
            continue

        column_series = df[column_name]
        checks = column_definition.get("checks", [])

        for check in checks:
            if not isinstance(check, dict) or len(check) != 1:
                continue

            check_name, check_config = next(iter(check.items()))

            if check_name == "missing":
                current_mask = column_series.isna() | (
                    column_series.astype(str).str.strip() == ""
                )
                quarantine_mask |= current_mask
                quarantine_reasons = append_reason(
                    quarantine_reasons, current_mask, f"{column_name}_missing"
                )

            elif check_name == "duplicate":
                non_missing_mask = ~(
                    column_series.isna() | (column_series.astype(str).str.strip() == "")
                )
                current_mask = column_series.duplicated(keep=False) & non_missing_mask
                quarantine_mask |= current_mask
                quarantine_reasons = append_reason(
                    quarantine_reasons, current_mask, f"{column_name}_duplicate"
                )

            elif check_name == "invalid" and isinstance(check_config, dict):
                current_mask = pd.Series(False, index=df.index)

                valid_values = check_config.get("valid_values")
                if valid_values is not None:
                    non_missing_mask = ~(
                        column_series.isna()
                        | (column_series.astype(str).str.strip() == "")
                    )
                    current_mask |= non_missing_mask & ~column_series.isin(valid_values)

                numeric_series = pd.to_numeric(column_series, errors="coerce")
                valid_min = check_config.get("valid_min")
                if valid_min is not None:
                    current_mask |= numeric_series.notna() & (numeric_series < valid_min)

                valid_max = check_config.get("valid_max")
                if valid_max is not None:
                    current_mask |= numeric_series.notna() & (numeric_series > valid_max)

                quarantine_mask |= current_mask
                quarantine_reasons = append_reason(
                    quarantine_reasons, current_mask, f"{column_name}_invalid"
                )

    df_valid = df[~quarantine_mask].copy()
    df_quarantine = df[quarantine_mask].copy()

    if not df_quarantine.empty:
        df_quarantine["quarantine_reason"] = quarantine_reasons.loc[df_quarantine.index]

    return df_valid, df_quarantine


def write_quality_reports(
    report_name: str,
    contract_path: str,
    total_rows: int,
    valid_rows: int,
    quarantine_rows: int,
    contract_passed: bool,
    df_quarantine: pd.DataFrame,
):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    try:
        os.chmod(REPORTS_DIR, 0o777)
    except PermissionError:
        print(f"Impossible de modifier les permissions de {REPORTS_DIR}")

    summary_rows = [
        {"report": report_name, "metric": "contract_path", "value": contract_path},
        {"report": report_name, "metric": "contract_passed", "value": contract_passed},
        {"report": report_name, "metric": "total_rows", "value": total_rows},
        {"report": report_name, "metric": "valid_rows", "value": valid_rows},
        {"report": report_name, "metric": "quarantine_rows", "value": quarantine_rows},
        {
            "report": report_name,
            "metric": "valid_ratio",
            "value": round((valid_rows / total_rows), 4) if total_rows else 0,
        },
    ]

    summary_df = pd.DataFrame(summary_rows)
    reasons_df = pd.DataFrame(columns=["quarantine_reason", "row_count"])
    if "quarantine_reason" in df_quarantine.columns and not df_quarantine.empty:
        reasons_df = (
            df_quarantine["quarantine_reason"]
            .fillna("")
            .str.split("|", regex=False)
            .explode()
            .loc[lambda series: series.ne("")]
            .value_counts()
            .rename_axis("quarantine_reason")
            .reset_index(name="row_count")
        )

    summary_csv_path = f"{REPORTS_DIR}/{report_name}_summary.csv"
    reasons_csv_path = f"{REPORTS_DIR}/{report_name}_reasons.csv"
    markdown_path = f"{REPORTS_DIR}/{report_name}_report.md"
    summary_df.to_csv(summary_csv_path, index=False)
    reasons_df.to_csv(reasons_csv_path, index=False)

    markdown_lines = [
        f"# {report_name}",
        "",
        f"- contract_path: `{contract_path}`",
        f"- contract_passed: `{contract_passed}`",
        f"- total_rows: `{total_rows}`",
        f"- valid_rows: `{valid_rows}`",
        f"- quarantine_rows: `{quarantine_rows}`",
        "",
        "## Top quarantine reasons",
        "",
    ]
    if reasons_df.empty:
        markdown_lines.append("No quarantine reasons.")
    else:
        markdown_lines.append("| quarantine_reason | row_count |")
        markdown_lines.append("|---|---:|")
        for row in reasons_df.itertuples(index=False):
            markdown_lines.append(f"| {row.quarantine_reason} | {row.row_count} |")

    with open(markdown_path, "w", encoding="utf-8") as markdown_file:
        markdown_file.write("\n".join(markdown_lines) + "\n")

    print(f"Rapports ecrits: {summary_csv_path}, {reasons_csv_path}, {markdown_path}")


def coerce_dataframe_for_contract(df: pd.DataFrame, contract_path: str) -> pd.DataFrame:
    contract = load_contract_definition(contract_path)
    coerced_df = df.copy()

    for column_definition in contract.get("columns", []):
        column_name = column_definition.get("name")
        if not column_name or column_name not in coerced_df.columns:
            continue

        checks = column_definition.get("checks", [])
        for check in checks:
            if not isinstance(check, dict) or len(check) != 1:
                continue

            check_name, check_config = next(iter(check.items()))
            if check_name == "invalid" and isinstance(check_config, dict):
                if "valid_min" in check_config or "valid_max" in check_config:
                    coerced_df[column_name] = pd.to_numeric(
                        coerced_df[column_name], errors="coerce"
                    )

    return coerced_df


def verify_contract(contract_path: str, fail_on_error: bool = True) -> bool:
    result = verify_contract_locally(
        data_source_file_path=SODA_CONFIG_PATH,
        contract_file_path=contract_path,
        publish=False,
    )

    get_logs = getattr(result, "get_logs", None)
    if callable(get_logs):
        for log_line in get_logs():
            print(log_line)

    if getattr(result, "has_errors", False):
        get_errors = getattr(result, "get_errors", None)
        if callable(get_errors):
            for error_line in get_errors():
                print(error_line)
        if fail_on_error:
            raise Exception(f"Contrat Soda en erreur: {contract_path}")
        return False

    if not getattr(result, "is_passed", False):
        if fail_on_error:
            raise Exception(f"Contrat Soda echoue: {contract_path}")
        return False

    return True


def validate_raw(**_context):
    df = pd.read_csv(RAW_EXTRACTED_CSV_PATH, dtype=str)
    df_for_contract = coerce_dataframe_for_contract(df, RAW_CONTRACT_PATH)
    replace_table_from_dataframe(df_for_contract, RAW_CONTRACT_TABLE)
    contract_passed = verify_contract(RAW_CONTRACT_PATH, fail_on_error=False)
    df_valid, df_quarantine = build_quarantine_from_contract(df, RAW_CONTRACT_PATH)
    write_quality_reports(
        report_name="raw_quality",
        contract_path=RAW_CONTRACT_PATH,
        total_rows=len(df),
        valid_rows=len(df_valid),
        quarantine_rows=len(df_quarantine),
        contract_passed=contract_passed,
        df_quarantine=df_quarantine,
    )
    print(f"Validation raw terminee: contrat_soda_ok={contract_passed}")


def materialize_valid_raw(**_context):
    df = pd.read_csv(RAW_EXTRACTED_CSV_PATH, dtype=str)
    df_valid, _ = build_quarantine_from_contract(df, RAW_CONTRACT_PATH)

    os.makedirs(os.path.dirname(RAW_CSV_PATH), exist_ok=True)
    df_valid.to_csv(RAW_CSV_PATH, index=False)
    print(f"Valid raw materialise: {len(df_valid)} lignes")


def materialize_quarantine_raw(**_context):
    df = pd.read_csv(RAW_EXTRACTED_CSV_PATH, dtype=str)
    _, df_quarantine = build_quarantine_from_contract(df, RAW_CONTRACT_PATH)

    os.makedirs(os.path.dirname(RAW_QUARANTINE_CSV_PATH), exist_ok=True)
    df_quarantine.to_csv(RAW_QUARANTINE_CSV_PATH, index=False)
    replace_table_from_dataframe(df_quarantine, RAW_QUARANTINE_TABLE)
    print(f"Quarantine raw materialisee: {len(df_quarantine)} lignes")


def validate_processed(**_context):
    df = pd.read_csv(CLEAN_CSV_PATH)
    df_for_contract = coerce_dataframe_for_contract(df, PROCESSED_CONTRACT_PATH)
    replace_table_from_dataframe(df_for_contract, PROCESSED_CONTRACT_TABLE)
    contract_passed = verify_contract(PROCESSED_CONTRACT_PATH, fail_on_error=False)
    df_valid, df_quarantine = build_quarantine_from_contract(
        df, PROCESSED_CONTRACT_PATH
    )
    write_quality_reports(
        report_name="processed_quality",
        contract_path=PROCESSED_CONTRACT_PATH,
        total_rows=len(df),
        valid_rows=len(df_valid),
        quarantine_rows=len(df_quarantine),
        contract_passed=contract_passed,
        df_quarantine=df_quarantine,
    )
    print(f"Validation processed terminee: contrat_soda_ok={contract_passed}")


def split_processed_outputs(**_context):
    df = pd.read_csv(CLEAN_CSV_PATH)
    df_valid, df_quarantine = build_quarantine_from_contract(
        df, PROCESSED_CONTRACT_PATH
    )

    os.makedirs(os.path.dirname(PROCESSED_VALID_CSV_PATH), exist_ok=True)
    df_valid.to_csv(PROCESSED_VALID_CSV_PATH, index=False)

    if not df_quarantine.empty:
        df_quarantine["quarantined_at"] = datetime.now()

    os.makedirs(os.path.dirname(PROCESSED_QUARANTINE_CSV_PATH), exist_ok=True)
    df_quarantine.to_csv(PROCESSED_QUARANTINE_CSV_PATH, index=False)
    print(
        "Split processed termine: "
        f"{len(df_valid)} lignes valides, {len(df_quarantine)} lignes en quarantaine"
    )


def materialize_valid_processed(**_context):
    df_valid = pd.read_csv(PROCESSED_VALID_CSV_PATH)
    replace_table_from_dataframe(df_valid, PROCESSED_VALID_TABLE)
    print(f"Processed valid materialise: {len(df_valid)} lignes")


def materialize_quarantine_processed(**_context):
    df_quarantine = pd.read_csv(PROCESSED_QUARANTINE_CSV_PATH)
    replace_table_from_dataframe(df_quarantine, PROCESSED_QUARANTINE_TABLE)
    print(f"Processed quarantine materialisee: {len(df_quarantine)} lignes")
