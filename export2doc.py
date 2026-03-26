#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "requests>=2.32.0",
#   "python-dotenv>=1.0.1",
#   "psycopg[binary]>=3.2.0",
#   "gspread>=6.1.2",
#   "google-auth>=2.35.0",
# ]
# ///

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import gspread
import psycopg
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

SOURCES = {"n8n", "gsheets", "supabase"}


class ExportError(Exception):
    pass


@dataclass
class Summary:
    exported: list[str]
    missing: list[str]


def eprint(message: str) -> None:
    print(message, file=sys.stderr)


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[\s_/]+", "-", value)
    value = re.sub(r"[^a-z0-9.-]+", "", value)
    value = re.sub(r"-+", "-", value).strip("-.")
    return value or "unnamed"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_text(path: Path, data: str) -> None:
    ensure_dir(path.parent)
    path.write_text(data, encoding="utf-8")


def split_csv_names(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    parts = [part.strip() for part in raw.split(",")]
    clean = [part for part in parts if part]
    return clean or []


def parse_cli(argv: list[str]) -> dict[str, list[str] | None]:
    if not argv:
        raise ExportError(
            "Uso: uv run export2doc.py n8n \"Fluxo A,Fluxo B\" gsheets \"Aba 1\" supabase \"users\""
        )

    plan: dict[str, list[str] | None] = {}
    i = 0
    while i < len(argv):
        token = argv[i].strip().lower()
        if token not in SOURCES:
            raise ExportError(f"Fonte inválida: {argv[i]}. Use apenas: n8n, gsheets, supabase.")

        names: list[str] | None = None
        if i + 1 < len(argv) and argv[i + 1].strip().lower() not in SOURCES:
            names = split_csv_names(argv[i + 1])
            i += 1

        if token in plan:
            existing = plan[token]
            if existing is None or names is None:
                plan[token] = None
            else:
                merged = existing + names
                deduped = list(dict.fromkeys(merged))
                plan[token] = deduped
        else:
            plan[token] = names

        i += 1

    return plan


def load_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ExportError(f"Variável obrigatória ausente no .env: {name}")
    return value


def build_output_root() -> Path:
    root = Path(os.getenv("EXPORT_OUTPUT_DIR", "./exports")).expanduser().resolve()
    ensure_dir(root)
    return root


class N8NClient:
    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "X-N8N-API-KEY": api_key,
            }
        )

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any] | list[Any]:
        url = f"{self.base_url}{path}"
        response = self.session.request(method, url, timeout=60, **kwargs)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body = response.text[:1000]
            raise ExportError(f"Erro n8n em {url}: {response.status_code} {body}") from exc
        return response.json()

    def list_workflows(self) -> list[dict[str, Any]]:
        workflows: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            params: dict[str, Any] = {"limit": 250}
            if cursor:
                params["cursor"] = cursor
            payload = self._request("GET", "/api/v1/workflows", params=params)
            if isinstance(payload, list):
                page_items = payload
                cursor = None
            else:
                page_items = payload.get("data") or payload.get("items") or []
                cursor = payload.get("nextCursor")

            for item in page_items:
                if isinstance(item, dict):
                    workflows.append(item)

            if not cursor:
                break

        return workflows

    def get_workflow(self, workflow_id: str | int) -> dict[str, Any]:
        payload = self._request("GET", f"/api/v1/workflows/{workflow_id}")
        if not isinstance(payload, dict):
            raise ExportError(f"Resposta inesperada ao buscar workflow {workflow_id}")
        return payload


def export_n8n(output_root: Path, selected_names: list[str] | None) -> Summary:
    base_url = load_required_env("N8N_BASE_URL")
    api_key = load_required_env("N8N_API_KEY")
    client = N8NClient(base_url=base_url, api_key=api_key)

    all_workflows = client.list_workflows()
    by_name = {wf.get("name", ""): wf for wf in all_workflows if wf.get("name")}
    out_dir = output_root / "n8n"
    ensure_dir(out_dir)

    exported: list[str] = []
    missing: list[str] = []

    if selected_names is None:
        detailed_workflows = []
        for workflow in all_workflows:
            workflow_id = workflow.get("id")
            if workflow_id is None:
                continue
            detailed = client.get_workflow(workflow_id)
            detailed_workflows.append(detailed)
            name = detailed.get("name") or str(workflow_id)
            write_json(out_dir / f"{slugify(name)}.json", detailed)
            exported.append(name)

        write_json(out_dir / "all.workflows.json", detailed_workflows)
        return Summary(exported=exported, missing=[])

    for requested_name in selected_names:
        workflow = by_name.get(requested_name)
        if workflow is None:
            # Fallback case-insensitive match.
            workflow = next(
                (wf for wf in all_workflows if str(wf.get("name", "")).lower() == requested_name.lower()),
                None,
            )
        if workflow is None:
            missing.append(requested_name)
            continue

        workflow_id = workflow.get("id")
        if workflow_id is None:
            missing.append(requested_name)
            continue

        detailed = client.get_workflow(workflow_id)
        actual_name = detailed.get("name") or requested_name
        write_json(out_dir / f"{slugify(actual_name)}.json", detailed)
        exported.append(actual_name)

    return Summary(exported=exported, missing=missing)


def build_gspread_client() -> gspread.Client:
    client_email = load_required_env("GOOGLE_CLIENT_EMAIL")
    private_key = load_required_env("GOOGLE_PRIVATE_KEY").replace("\\n", "\n")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = Credentials.from_service_account_info(
        {
            "type": "service_account",
            "client_email": client_email,
            "private_key": private_key,
            # The remaining fields are optional for this auth flow with google-auth.
            "token_uri": os.getenv("GOOGLE_TOKEN_URI", "https://oauth2.googleapis.com/token"),
        },
        scopes=scopes,
    )
    return gspread.authorize(credentials)


def worksheet_to_records(worksheet: gspread.Worksheet) -> dict[str, Any]:
    values = worksheet.get_all_values()
    if not values:
        headers: list[str] = []
        rows: list[dict[str, Any]] = []
    else:
        headers = values[0]
        rows = []
        for row_values in values[1:]:
            padded = row_values + [""] * max(0, len(headers) - len(row_values))
            rows.append(dict(zip(headers, padded)))

    return {
        "title": worksheet.title,
        "worksheet_id": worksheet.id,
        "row_count": worksheet.row_count,
        "col_count": worksheet.col_count,
        "headers": headers,
        "rows": rows,
    }


def export_gsheets(output_root: Path, selected_sheet_names: list[str] | None) -> Summary:
    sheet_id = load_required_env("GOOGLE_SHEET_ID")
    gc = build_gspread_client()
    spreadsheet = gc.open_by_key(sheet_id)
    worksheets = spreadsheet.worksheets()
    out_dir = output_root / "gsheets"
    ensure_dir(out_dir)

    worksheet_map = {ws.title: ws for ws in worksheets}
    exported: list[str] = []
    missing: list[str] = []

    if selected_sheet_names is None:
        workbook_payload = {
            "spreadsheet_id": spreadsheet.id,
            "title": spreadsheet.title,
            "worksheets": [],
        }
        for worksheet in worksheets:
            payload = worksheet_to_records(worksheet)
            workbook_payload["worksheets"].append(payload)
            write_json(out_dir / f"{slugify(worksheet.title)}.json", payload)
            exported.append(worksheet.title)

        write_json(out_dir / "workbook.json", workbook_payload)
        return Summary(exported=exported, missing=[])

    for requested_name in selected_sheet_names:
        worksheet = worksheet_map.get(requested_name)
        if worksheet is None:
            worksheet = next((ws for ws in worksheets if ws.title.lower() == requested_name.lower()), None)
        if worksheet is None:
            missing.append(requested_name)
            continue

        payload = worksheet_to_records(worksheet)
        write_json(out_dir / f"{slugify(worksheet.title)}.json", payload)
        exported.append(worksheet.title)

    return Summary(exported=exported, missing=missing)


def fetch_table_schema(conn: psycopg.Connection[Any], table_names: list[str] | None) -> list[dict[str, Any]]:
    filters = ""
    params: list[Any] = []
    if table_names is not None:
        filters = "AND c.table_name = ANY(%s)"
        params.append(table_names)

    query = f"""
        SELECT
            c.table_name,
            c.column_name,
            c.data_type,
            c.udt_name,
            c.is_nullable,
            c.column_default,
            c.ordinal_position,
            pgd.description AS column_comment
        FROM information_schema.columns c
        LEFT JOIN pg_catalog.pg_statio_all_tables st
            ON st.relname = c.table_name
        LEFT JOIN pg_catalog.pg_description pgd
            ON pgd.objoid = st.relid
           AND pgd.objsubid = c.ordinal_position
        WHERE c.table_schema = 'public'
          {filters}
        ORDER BY c.table_name, c.ordinal_position
    """

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        colnames = [desc.name for desc in cur.description]

    by_table: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = dict(zip(colnames, row))
        table_name = str(item["table_name"])
        table = by_table.setdefault(table_name, {"table_name": table_name, "columns": []})
        table["columns"].append(
            {
                "name": item["column_name"],
                "data_type": item["data_type"],
                "udt_name": item["udt_name"],
                "is_nullable": item["is_nullable"] == "YES",
                "default": item["column_default"],
                "ordinal_position": item["ordinal_position"],
                "comment": item["column_comment"],
            }
        )

    return list(by_table.values())


def to_create_table_sql(table: dict[str, Any]) -> str:
    lines = []
    for col in table["columns"]:
        parts = [f'  "{col["name"]}" {col["data_type"]}']
        if not col["is_nullable"]:
            parts.append("NOT NULL")
        if col["default"] is not None:
            parts.append(f"DEFAULT {col['default']}")
        lines.append(" ".join(parts))
    cols_sql = ",\n".join(lines)
    return f'CREATE TABLE public."{table["table_name"]}" (\n{cols_sql}\n);'


def export_supabase(output_root: Path, selected_tables: list[str] | None) -> Summary:
    db_url = load_required_env("SUPABASE_DB_URL")
    out_dir = output_root / "supabase"
    ensure_dir(out_dir)

    with psycopg.connect(db_url) as conn:
        tables = fetch_table_schema(conn, selected_tables)
        found_names = [table["table_name"] for table in tables]

    exported: list[str] = []
    missing: list[str] = []

    if selected_tables is not None:
        found_lower = {name.lower() for name in found_names}
        missing = [name for name in selected_tables if name.lower() not in found_lower]

    if selected_tables is None:
        bundle = {"schema": "public", "tables": tables}
        write_json(out_dir / "schema-all.json", bundle)
        sql = "\n\n".join(to_create_table_sql(table) for table in tables)
        write_text(out_dir / "schema-all.sql", sql + ("\n" if sql else ""))

    for table in tables:
        exported.append(table["table_name"])
        write_json(out_dir / f'{slugify(table["table_name"])}.json', table)

    return Summary(exported=exported, missing=missing)


def print_summary(name: str, summary: Summary) -> None:
    print(f"[OK] {name}: {len(summary.exported)} item(ns) exportado(s)")
    for item in summary.exported:
        print(f"  - {item}")
    for item in summary.missing:
        print(f"[WARN] {name}: não encontrado -> {item}")


def main(argv: list[str]) -> int:
    load_dotenv()
    try:
        plan = parse_cli(argv)
        output_root = build_output_root()

        if "n8n" in plan:
            print_summary("n8n", export_n8n(output_root, plan["n8n"]))

        if "gsheets" in plan:
            print_summary("gsheets", export_gsheets(output_root, plan["gsheets"]))

        if "supabase" in plan:
            print_summary("supabase", export_supabase(output_root, plan["supabase"]))

        print(f"\nSaída em: {output_root}")
        return 0
    except KeyboardInterrupt:
        eprint("Execução interrompida.")
        return 130
    except ExportError as exc:
        eprint(f"Erro: {exc}")
        return 2
    except Exception as exc:
        eprint(f"Erro inesperado: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
