# -*- coding: utf-8 -*-
"""KoboCollectData.py

Ejecución automática para descargar datos de Kobo y subirlos a Google Sheets.
"""

#!/usr/bin/env python3
import os
import json
import re
import requests
import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials
import gspread

# ===== CONFIGURACIÓN =====
KOBO_URL = "https://kf.kobotoolbox.org/assets/axWwJY5A9AeyzcJPtjACaf/submissions/?format=json"
OUTPUT_FOLDER = "output"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "2_ReporteInfCampo.xlsx")
CREDENTIALS_FILE = "credentials.json"
SHEET_ID = "1uhpIYhuFhfYJlHuJKq1VDsj9jFPXS4iW2qxdyPL4aiA"  # <-- reemplazar por tu ID real

# Columnas clave para expansión de empleados
EMPLOYEE_KEYWORDS = ["TiqueteCajon", "TiqueteCable", "OperariosCosecha"]

# ===== UTILIDADES =====
def sanitize_sheet_name(name: str, maxlen: int = 31) -> str:
    if not isinstance(name, str) or not name:
        name = "sheet"
    cleaned = re.sub(r'[\/\\\?\*\[\]\:]', '_', name)
    cleaned = re.sub(r'\s+', '_', cleaned)[:maxlen]
    return cleaned

def safe_serialize(value):
    if pd.isna(value):
        return ""
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    if isinstance(value, (np.generic,)):
        return np.asscalar(value) if hasattr(np, "asscalar") else str(value)
    return value

# ===== DESCARGA (paginada/respuesta lista) =====
def get_all_submissions(url, headers=None):
    all_results = []
    next_url = url
    session = requests.Session()
    while next_url:
        print(f"📥 Descargando: {next_url}")
        resp = session.get(next_url, headers=headers or {})
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            results = data.get("results", [])
            all_results.extend(results)
            next_url = data.get("next")
        elif isinstance(data, list):
            all_results.extend(data)
            next_url = None
        else:
            print("⚠ Respuesta inesperada de la API - tipo desconocido")
            next_url = None
    return all_results

# ===== SEPARAR CAMPOS ANIDADOS Y EXPANDIR EMPLEADOS =====
def split_nested_data(df: pd.DataFrame, parent_name="Main"):
    sub_dfs = {}
    for col in list(df.columns):
        mask = df[col].apply(lambda x: isinstance(x, (list, dict, str)))
        if mask.any():
            rows = []
            for idx, val in df[col].items():
                row_series = df.loc[idx]
                parent_id = row_series.get("_id", idx)

                # ¿Columna de empleados?
                expand_employees = any(k in col for k in EMPLOYEE_KEYWORDS)

                if isinstance(val, list):
                    for i, item in enumerate(val):
                        if isinstance(item, dict):
                            if expand_employees:
                                for key, emp_val in item.items():
                                    if isinstance(emp_val, str) and " " in emp_val:
                                        for emp in emp_val.split():
                                            new_row = {"parent_id": parent_id, "item_index": i}
                                            new_row.update({k: v for k, v in item.items() if k != key})
                                            new_row[key] = emp
                                            rows.append(new_row)
                                    else:
                                        new_row = {"parent_id": parent_id, "item_index": i}
                                        new_row.update(item)
                                        rows.append(new_row)
                            else:
                                row = {"parent_id": parent_id, "item_index": i}
                                row.update(item)
                                rows.append(row)
                        else:
                            row = {"parent_id": parent_id, "item_index": i, "value": item}
                            rows.append(row)

                elif isinstance(val, dict):
                    if expand_employees:
                        for key, emp_val in val.items():
                            if isinstance(emp_val, str) and " " in emp_val:
                                for emp in emp_val.split():
                                    new_row = {"parent_id": parent_id}
                                    new_row.update({k: v for k, v in val.items() if k != key})
                                    new_row[key] = emp
                                    rows.append(new_row)
                            else:
                                new_row = {"parent_id": parent_id}
                                new_row.update(val)
                                rows.append(new_row)
                    else:
                        row = {"parent_id": parent_id}
                        row.update(val)
                        rows.append(row)

                elif isinstance(val, str) and expand_employees:
                    # Cadena con múltiples empleados separados por espacios
                    for emp in val.split():
                        row = {"parent_id": parent_id}
                        # Copiar todos los demás campos de la fila excepto esta columna
                        for k, v in row_series.items():
                            if k != col:
                                row[k] = v
                        row[col] = emp
                        rows.append(row)

            if rows:
                sub_name = f"{parent_name}_{col}"
                sub_df = pd.DataFrame(rows)
                sub_df = sub_df.replace([np.inf, -np.inf], np.nan).fillna("")
                sub_df = sub_df.applymap(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
                sub_dfs[sub_name] = sub_df

            # En el df principal dejamos serializado lo complejo
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else ("" if pd.isna(x) else x)
            )
    return df, sub_dfs

# ===== GUARDAR A EXCEL =====
def save_to_excel(dfs: dict, filename: str):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        for name, df in dfs.items():
            sheet_name = sanitize_sheet_name(name, maxlen=31)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"✅ Archivo Excel generado con {dfs.get('Main').shape[0] if 'Main' in dfs else 0} registros en:\n{filename}")

# ===== SUBIR A GOOGLE SHEETS (INCREMENTAL) =====
def upload_to_google_sheets(dfs: dict, sheet_id: str, creds_file: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    for name, df in dfs.items():
        df_clean = df.replace([np.inf, -np.inf], np.nan).fillna("")
        df_clean = df_clean.applymap(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
        sheet_name = sanitize_sheet_name(name, maxlen=100)

        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            existing_data = worksheet.get_all_records()
            existing_df = pd.DataFrame(existing_data)

            if name == "Main":
                if "_id" in df_clean.columns and "_id" in existing_df.columns:
                    new_df = df_clean[~df_clean["_id"].astype(str).isin(existing_df["_id"].astype(str))]
                elif "submission_id" in df_clean.columns and "submission_id" in existing_df.columns:
                    new_df = df_clean[~df_clean["submission_id"].astype(str).isin(existing_df["submission_id"].astype(str))]
                else:
                    new_df = df_clean
            else:
                if "parent_id" in df_clean.columns and "parent_id" in existing_df.columns:
                    if "item_index" in df_clean.columns and "item_index" in existing_df.columns:
                        merged = existing_df[["parent_id", "item_index"]].astype(str).agg("_".join, axis=1)
                        current = df_clean[["parent_id", "item_index"]].astype(str).agg("_".join, axis=1)
                        new_df = df_clean[~current.isin(merged)]
                    else:
                        new_df = df_clean[~df_clean["parent_id"].astype(str).isin(existing_df["parent_id"].astype(str))]
                else:
                    new_df = df_clean
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name,
                                                  rows=max(1, df_clean.shape[0] + 1),
                                                  cols=max(1, df_clean.shape[1]))
            worksheet.update([df_clean.columns.values.tolist()])
            new_df = df_clean

        if not new_df.empty:
            worksheet.append_rows(new_df.values.tolist())
            print(f"📤 Se agregaron {new_df.shape[0]} registros nuevos en '{sheet_name}'")
        else:
            print(f"ℹ No hay registros nuevos para '{sheet_name}'")

# ===== FLUJO PRINCIPAL =====
def main():
    results = get_all_submissions(KOBO_URL)
    if not results:
        print("⚠ No se encontraron registros en Kobo.")
        return

    df_main = pd.DataFrame(results)
    if "_id" in df_main.columns:
        df_main["submission_id"] = df_main["_id"]
    else:
        df_main["submission_id"] = df_main.index.astype(str)

    df_main, sub_dfs = split_nested_data(df_main, parent_name="Main")
    dfs = {"Main": df_main}
    dfs.update(sub_dfs)

    save_to_excel(dfs, OUTPUT_FILE)
    upload_to_google_sheets(dfs, SHEET_ID, CREDENTIALS_FILE)

if __name__ == "__main__":
    main()
