"""
Script separato per aggregazione mensile bollette.

Questo script fa solo l'aggregazione mensile.
Prende `estrazione_tutti_mesi.csv` e genera `bollette_raggruppate.xlsx`.

Uso:
    python src/pipeline/aggregate_bills.py [input_csv] [output_xlsx]

Esempi:
    python src/pipeline/aggregate_bills.py
    python src/pipeline/aggregate_bills.py estrazione_tutti_mesi.csv bollette_raggruppate.xlsx

Se non specificato, usa i file nella root del progetto.
"""

import argparse
import csv
import re

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# ========= CONFIG =========
root = Path(__file__).resolve().parents[2]
INPUT_PATH = root / "estrazione_tutti_mesi.csv"
OUT_XLSX = root / "bollette_raggruppate.xlsx"


# ========= UTILS =========
def clean_colname(value: str) -> str:
    if value is None:
        return ""
    value = re.sub(r"[\uFEFF\u200B\u200C\u200D]", "", str(value))
    value = value.replace("\n", " ").strip()
    value = re.sub(r"\s+", " ", value)
    return value.lower()


def sniff_sep(sample_path: str | Path) -> str:
    with open(sample_path, "r", encoding="utf-8", errors="ignore") as file_obj:
        sample = file_obj.read(4096)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return "," if sample.count(",") >= sample.count(";") else ";"


def read_table(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(path, dtype=str)
    else:
        sep = sniff_sep(path)
        df = pd.read_csv(path, sep=sep, engine="python", dtype=str)
    return df.rename(columns=lambda col: clean_colname(col))


def find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    columns = list(df.columns)
    for candidate in candidates:
        clean_candidate = clean_colname(candidate)
        if clean_candidate in columns:
            return clean_candidate

    for candidate in candidates:
        clean_candidate = clean_colname(candidate)
        for column in columns:
            if column.startswith(clean_candidate) or clean_candidate in column:
                return column
    return None


def to_float(value):
    if pd.isna(value):
        return np.nan
    text = str(value).strip()
    if text == "":
        return np.nan
    if "." in text and "," in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    text = re.sub(r"[^0-9\.\-]", "", text)
    if text in ("", "-", ".", "-."):
        return np.nan
    try:
        return float(text)
    except ValueError:
        return np.nan


def month_name_it(month_number: int) -> str:
    months = {
        1: "gennaio",
        2: "febbraio",
        3: "marzo",
        4: "aprile",
        5: "maggio",
        6: "giugno",
        7: "luglio",
        8: "agosto",
        9: "settembre",
        10: "ottobre",
        11: "novembre",
        12: "dicembre",
    }
    return months.get(int(month_number), "")


def consumo_moda(series: pd.Series) -> float:
    """
    Non sommare: consumo del mese come moda.
    - Se unica moda -> quella
    - Se piu mode -> prendi la prima
    - Se non ci sono numeri -> NaN
    - Fallback: ultimo non-NaN
    """
    values = series.dropna().astype(float).round(6)
    if values.empty:
        return np.nan
    mode = values.mode()
    if len(mode) >= 1:
        return float(mode.iloc[0])
    return float(values.iloc[-1])


def is_si(value: str) -> bool:
    """Return True if the text is equivalent to 'si'."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    text = str(value).strip().lower()
    text = text.replace("\u00ec", "i")
    return text in {"si", "yes", "true", "1", "y"}


def build_arg_parser() -> argparse.ArgumentParser:
    """Create CLI parser for the standalone aggregation script."""
    parser = argparse.ArgumentParser(
        description="Aggrega i dati estratti in un riepilogo mensile."
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        help='CSV/XLSX di input. Se omesso usa "estrazione_tutti_mesi.csv" nella root progetto.',
    )
    parser.add_argument(
        "output_xlsx",
        nargs="?",
        help='XLSX di output. Se omesso usa "bollette_raggruppate.xlsx" nella root progetto.',
    )
    return parser


def resolve_input_path(input_path_arg: Optional[str]) -> tuple[Path, list[Path]]:
    """Resolve input path from CLI, supporting cwd-relative and project-relative paths."""
    if input_path_arg:
        raw_path = Path(input_path_arg).expanduser()
        if raw_path.is_absolute():
            return raw_path, [raw_path]

        candidate_paths: list[Path] = []
        for base_path in (Path.cwd(), root):
            candidate = (base_path / raw_path).resolve()
            if candidate not in candidate_paths:
                candidate_paths.append(candidate)

        for candidate in candidate_paths:
            if candidate.exists():
                return candidate, candidate_paths

        return candidate_paths[-1], candidate_paths

    default_candidates = [INPUT_PATH]
    cwd_default = (Path.cwd() / INPUT_PATH.name).resolve()
    if cwd_default not in default_candidates:
        default_candidates.append(cwd_default)

    for candidate in default_candidates:
        if candidate.exists():
            return candidate, default_candidates

    return INPUT_PATH, default_candidates


def resolve_output_path(output_path_arg: Optional[str]) -> Path:
    """Resolve output path, defaulting to the project root file."""
    if not output_path_arg:
        return OUT_XLSX

    raw_path = Path(output_path_arg).expanduser()
    if raw_path.is_absolute():
        return raw_path

    return (Path.cwd() / raw_path).resolve()


def aggregate_bolletta_data(
    input_path: str | Path = INPUT_PATH,
    out_xlsx: str | Path = OUT_XLSX,
) -> Optional[pd.DataFrame]:
    """
    Aggrega i dati estratti delle bollette in un riepilogo mensile.

    Args:
        input_path: Path al CSV/XLSX di input
        out_xlsx: Path al XLSX di output

    Returns:
        DataFrame aggregato, o None se errore
    """
    input_path = Path(input_path)
    out_xlsx = Path(out_xlsx)

    df = read_table(input_path)

    col_importo = find_col(df, ["importo", "importi", "valore", "totale_riga"])
    col_imponibile = find_col(df, ["imponibile_mese", "imponibile mese", "imponibile"])
    col_cons = find_col(df, ["consumo_totale", "consumo", "consumi"])
    col_manca_dett = find_col(
        df,
        ["manca_dettaglio", "manca dettaglio", "senza_dettaglio", "senza dettaglio"],
    )
    col_voce = find_col(df, ["dettaglio_voce", "voce", "descrizione", "descrizione_voce"])
    col_data = (
        find_col(df, ["data_fine"])
        or find_col(df, ["data_inizio"])
        or find_col(df, ["data_fattura"])
        or find_col(df, ["data"])
    )

    missing = []
    if not col_cons:
        missing.append("consumo_totale/consumo")
    if not col_data:
        missing.append("data_fine/data_inizio/data_fattura/data")
    if not col_importo and not col_imponibile:
        missing.append("importo o imponibile_mese")
    if missing:
        raise ValueError("Colonne mancanti nel file: " + ", ".join(missing))

    print(
        "Colonne usate ->",
        f"consumo: '{col_cons}', data: '{col_data}',",
        f"importo: '{col_importo}'" if col_importo else "importo: (assente)",
        f"imponibile_mese: '{col_imponibile}'" if col_imponibile else "imponibile_mese: (assente)",
        f"manca_dettaglio: '{col_manca_dett}'" if col_manca_dett else "manca_dettaglio: (assente)",
        f"voce: '{col_voce}'" if col_voce else "voce: (assente)",
    )

    if col_importo:
        df[col_importo] = df[col_importo].apply(to_float)
    if col_imponibile:
        df[col_imponibile] = df[col_imponibile].apply(to_float)
    df[col_cons] = df[col_cons].apply(to_float)
    df[col_data] = pd.to_datetime(df[col_data], dayfirst=True, errors="coerce")

    if col_manca_dett:
        df["_manca_det_bool"] = df[col_manca_dett].apply(is_si)
    else:
        df["_manca_det_bool"] = False

    rows_before = len(df)
    df = df.dropna(subset=[col_data]).copy()
    rows_after = len(df)
    if rows_after < rows_before:
        print(f"Righe senza data valida scartate: {rows_before - rows_after}")

    df["anno"] = df[col_data].dt.year
    df["mese_num"] = df[col_data].dt.month
    df["mese"] = df["mese_num"].apply(month_name_it)

    def agg_per_mese(group: pd.DataFrame) -> pd.Series:
        consumo = consumo_moda(group[col_cons])
        manca_det_mese = bool(group["_manca_det_bool"].any())

        if manca_det_mese:
            if col_imponibile and col_imponibile in group.columns:
                imponibile_values = group[col_imponibile].dropna()
                importo_finale = (
                    float(imponibile_values.iloc[0]) if not imponibile_values.empty else np.nan
                )
            elif col_importo and col_importo in group.columns:
                importo_values = group[col_importo].dropna()
                importo_finale = float(importo_values.iloc[-1]) if not importo_values.empty else np.nan
            else:
                importo_finale = np.nan
        else:
            if col_importo and col_importo in group.columns:
                importo_finale = float(group[col_importo].sum())
            elif col_imponibile and col_imponibile in group.columns:
                imponibile_values = group[col_imponibile].dropna()
                importo_finale = (
                    float(imponibile_values.iloc[0]) if not imponibile_values.empty else np.nan
                )
            else:
                importo_finale = np.nan

        consumo_nuniq = group[col_cons].dropna().astype(float).round(6).nunique()
        imponibile_nuniq = (
            group[col_imponibile].dropna().nunique()
            if col_imponibile and col_imponibile in group.columns
            else np.nan
        )

        return pd.Series(
            {
                "totale_importi": importo_finale,
                "consumo_mese": consumo,
                "manca_dettaglio_mese": manca_det_mese,
                "consumo_valori_distinti": consumo_nuniq,
                "imponibile_valori_distinti": imponibile_nuniq,
                "righe": len(group),
            }
        )

    agg = (
        df.groupby(["anno", "mese_num"], sort=True)
        .apply(agg_per_mese, include_groups=False)
        .reset_index()
    )
    agg["mese"] = agg["mese_num"].apply(month_name_it)
    agg = agg.sort_values(["anno", "mese_num"]).reset_index(drop=True)

    out = agg[["anno", "mese", "totale_importi", "consumo_mese"]].copy()
    out["totale_importi"] = out["totale_importi"].map(
        lambda value: "" if pd.isna(value) else f"{value:.2f}".replace(".", ",")
    )
    out["consumo_mese"] = out["consumo_mese"].map(
        lambda value: "" if pd.isna(value) else str(value).replace(".", ",")
    )
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    out.to_excel(out_xlsx, index=False)
    print(f"Esportato: {out_xlsx}")
    return agg


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entrypoint for standalone aggregation."""
    args = build_arg_parser().parse_args(argv)
    input_path, searched_paths = resolve_input_path(args.input_csv)
    output_path = resolve_output_path(args.output_xlsx)

    if not input_path.exists():
        searched = "\n".join(f"- {path}" for path in searched_paths)
        print(f"Input non trovato. Percorsi controllati:\n{searched}")
        return 1

    if not input_path.is_file():
        print(f"Il percorso di input non e un file: {input_path}")
        return 1

    df = aggregate_bolletta_data(input_path, output_path)
    if df is not None:
        print(f"Aggregazione completata: {len(df)} mesi riepilogati")
        print(df.head(10))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
