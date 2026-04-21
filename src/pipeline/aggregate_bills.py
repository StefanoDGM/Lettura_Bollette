"""
Aggregazione mensile delle righe estratte dalle bollette.

Questo step lavora SOLO a valle dell'estrazione completa:
- legge CSV/XLSX con tutte le righe
- ricostruisce importi e consumi per mese di competenza
- spalma eventuali ricalcoli aggregati solo nel post-processing finale
- produce warning espliciti per i casi ambigui
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH = ROOT / "estrazione_tutti_mesi.csv"
OUT_XLSX = ROOT / "bollette_raggruppate.xlsx"

MONTH_NAMES = {
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

CORE_COLUMNS = [
    "data_inizio",
    "data_fine",
    "consumo_totale",
    "consumo_dettaglio_riga",
    "quantita",
    "unita_misura",
    "importo",
    "imponibile_mese",
    "manca_dettaglio",
    "manca_dettaglio_consumo",
    "_source_file",
    "dettaglio_voce",
    "tipo_componente",
    "riferimento_ricalcolo_da",
    "riferimento_ricalcolo_a",
    "presenza_ricalcolo",
    "ricalcolo_aggregato_multi_mese",
]
WARNING_FLAG_COLUMNS = [
    "ricalcolo_presente",
    "ricalcolo_aggregato_multi_mese",
    "dettaglio_ricostruzione_presente",
    "totale_documento_non_confrontabile_direttamente_con_mese_corrente",
    "nessun_ricalcolo_rilevato_imponibile_usato_come_fonte_finale",
    "mismatch_dettaglio_vs_imponibile_senza_ricalcoli",
    "allocazione_fallback_dividi_per_due",
    "verifica_manuale_consigliata",
]
DETAIL_VS_IMPONIBILE_TOLERANCE = 0.5
VALID_CATEGORIE_PARSER = {
    "riga_analitica_mese",
    "evento_ricalcolo",
    "evento_storno",
    "evento_acconto_precedente",
    "totale_aggregato_multi_mese",
    "tabella_supporto_consumi_mensili",
}
RICALCOLO_KEYWORDS = (
    "ricalcolo",
    "conguaglio",
    "importi gia contabilizzati",
    "importi già contabilizzati",
    "bollette precedenti",
    "fatture precedenti",
    "ex art. 6.2",
)
STORNO_KEYWORDS = ("storno", "rettifica", "accredito")
ACCONTO_PRECEDENTE_KEYWORDS = ("acconto", "acconti")
RECALC_EVENT_CATEGORIES = {
    "evento_ricalcolo",
    "evento_storno",
    "evento_acconto_precedente",
    "totale_aggregato_multi_mese",
}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aggrega le righe bolletta per mese di competenza."
    )
    parser.add_argument("input_path", nargs="?", help="CSV/XLSX di estrazione.")
    parser.add_argument("output_path", nargs="?", help="XLSX aggregato da generare.")
    return parser


def resolve_input_path(input_path_arg: Optional[str]) -> tuple[Path, list[Path]]:
    if input_path_arg:
        raw_path = Path(input_path_arg).expanduser()
        if raw_path.is_absolute():
            return raw_path, [raw_path]

        searched_paths: list[Path] = []
        for base in (Path.cwd(), ROOT):
            candidate = (base / raw_path).resolve()
            if candidate not in searched_paths:
                searched_paths.append(candidate)
            if candidate.exists():
                return candidate, searched_paths
        return searched_paths[-1], searched_paths

    return INPUT_PATH, [INPUT_PATH]


def resolve_output_path(output_path_arg: Optional[str]) -> Path:
    if not output_path_arg:
        return OUT_XLSX

    raw_path = Path(output_path_arg).expanduser()
    if raw_path.is_absolute():
        return raw_path
    return (Path.cwd() / raw_path).resolve()


def sniff_sep(path: Path) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as file_obj:
        sample = file_obj.read(4096)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except csv.Error:
        return ","


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str)
    return pd.read_csv(path, dtype=str, sep=sniff_sep(path))


def parse_number(value) -> float | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    else:
        text = text.replace(",", ".")

    cleaned = "".join(ch for ch in text if ch in "0123456789.-")
    if cleaned in {"", "-", ".", "-."}:
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_bool_si_no(value) -> bool | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"si", "sì", "yes", "true", "1"}:
        return True
    if text in {"no", "false", "0"}:
        return False
    return None


def bool_to_si_no(value: bool) -> str:
    return "si" if value else "no"


def empty_warning_flags() -> dict[str, str]:
    return {column: "no" for column in WARNING_FLAG_COLUMNS}


def mark_warning_flag(flags: dict[str, str], flag_name: str) -> None:
    if flag_name in flags:
        flags[flag_name] = "si"


def merge_warning_flags(*flag_groups: dict[str, str]) -> dict[str, str]:
    merged = empty_warning_flags()
    for flag_group in flag_groups:
        for flag_name in WARNING_FLAG_COLUMNS:
            if flag_group.get(flag_name) == "si":
                merged[flag_name] = "si"
    return merged


def normalized_text(*values) -> str:
    return " | ".join(
        str(value).strip().lower()
        for value in values
        if value is not None and not pd.isna(value) and str(value).strip()
    )


def parse_date(value) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return pd.NaT
    text = str(value).strip()
    if not text:
        return pd.NaT
    return pd.to_datetime(text, dayfirst=True, errors="coerce")


def month_start(timestamp: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=timestamp.year, month=timestamp.month, day=1)


def month_range(start_date: pd.Timestamp, end_date: pd.Timestamp) -> list[pd.Timestamp]:
    if pd.isna(start_date) or pd.isna(end_date):
        return []
    current = month_start(start_date)
    end = month_start(end_date)
    months: list[pd.Timestamp] = []
    while current <= end:
        months.append(current)
        current = current + pd.offsets.MonthBegin(1)
    return months


def is_full_month_interval(start_date: pd.Timestamp, end_date: pd.Timestamp) -> bool:
    if pd.isna(start_date) or pd.isna(end_date):
        return False
    return (
        start_date.day == 1
        and end_date == (month_start(end_date) + pd.offsets.MonthEnd(0))
    )


def month_label(year: int, month: int) -> str:
    return MONTH_NAMES.get(month, str(month))


def normalize_component(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().lower()


def has_true_multi_month_scope(row: pd.Series) -> bool:
    return len(month_range(row.get("_rif_da_dt"), row.get("_rif_a_dt"))) > 1


def is_recalc_event_category(category: str) -> bool:
    return str(category or "").strip().lower() in RECALC_EVENT_CATEGORIES


def is_row_multi_month_recalc(row: pd.Series) -> bool:
    category = str(row.get("_categoria_parser", "") or "").strip().lower()
    return category == "totale_aggregato_multi_mese" or has_true_multi_month_scope(row)


def infer_tipo_componente(row: pd.Series) -> str:
    raw = normalize_component(row.get("tipo_componente"))
    if raw == "ricalcolo_aggregato" and not is_row_multi_month_recalc(row):
        raw = ""
    if raw in {"fissa", "variabile", "imposte", "trasporto", "oneri", "ricalcolo_aggregato"}:
        return raw

    text = normalized_text(row.get("dettaglio_voce"), row.get("note"))
    unit = str(row.get("unita_misura", "") or "").strip().lower()

    if any(keyword in text for keyword in ("imposta", "accisa", "addizionale")):
        return "imposte"
    if any(keyword in text for keyword in ("crvbl", "crvi", "cvu", "trasporto", "contatore")):
        return "trasporto"
    if "oneri" in text:
        return "oneri"
    if "quota fissa" in text or "/mese" in unit or "mese/ig" in unit:
        return "fissa"
    if any(keyword in text for keyword in ("quota proporzionale", "consumi", "quota energia")) and "/smc" in unit:
        return "variabile"
    if "/smc" in unit and raw != "trasporto":
        return "variabile"
    return raw or "altro"


def unique_nonempty_texts(series: pd.Series) -> list[str]:
    values = []
    for value in series.dropna().astype(str):
        text = value.strip()
        if text and text not in values:
            values.append(text)
    return values


def split_warning_text(text: str) -> list[str]:
    if not text:
        return []
    return [part.strip() for part in str(text).split("|") if part.strip()]


def unique_numbers_in_order(series: pd.Series) -> list[float]:
    values: list[float] = []
    for raw_value in series.dropna().tolist():
        value = float(raw_value)
        if value not in values:
            values.append(value)
    return values


def infer_categoria_parser(row: pd.Series) -> str:
    raw_category = str(row.get("categoria_parser", "") or "").strip().lower()
    text = normalized_text(row.get("dettaglio_voce"), row.get("note"))
    rif_da = row.get("_rif_da_dt")
    rif_a = row.get("_rif_a_dt")

    if raw_category == "tabella_supporto_consumi_mensili":
        return raw_category

    if "consumi relativi agli ultimi mesi" in text and pd.isna(row.get("_importo_num")):
        return "tabella_supporto_consumi_mensili"

    if has_true_multi_month_scope(row):
        return "totale_aggregato_multi_mese"

    if raw_category in VALID_CATEGORIE_PARSER and raw_category != "totale_aggregato_multi_mese":
        return raw_category

    if any(keyword in text for keyword in ACCONTO_PRECEDENTE_KEYWORDS) and (
        "precedent" in text or "gia contabilizzati" in text or "già contabilizzati" in text
    ):
        return "evento_acconto_precedente"

    if any(keyword in text for keyword in STORNO_KEYWORDS):
        return "evento_storno"

    if any(keyword in text for keyword in RICALCOLO_KEYWORDS) or pd.notna(rif_da) or pd.notna(rif_a):
        return "evento_ricalcolo"

    return "riga_analitica_mese"


def finalize_document_flags(prepared: pd.DataFrame) -> pd.DataFrame:
    for column in (
        "presenza_ricalcolo",
        "ricalcolo_aggregato_multi_mese",
        "dettaglio_ricostruzione_presente",
        "totale_documento_puo_non_coincidere_con_mese_corrente",
        "categoria_parser",
    ):
        if column not in prepared.columns:
            prepared[column] = ""

    prepared["categoria_parser"] = prepared.apply(infer_categoria_parser, axis=1)
    prepared["_categoria_parser"] = prepared["categoria_parser"].fillna("").astype(str).str.strip().str.lower()

    prepared["_presenza_ricalcolo_bool"] = prepared["_categoria_parser"].apply(is_recalc_event_category)
    prepared["_ricalcolo_multi_bool"] = prepared.apply(is_row_multi_month_recalc, axis=1)
    prepared["_dettaglio_ricostruzione_bool"] = prepared["dettaglio_ricostruzione_presente"].apply(parse_bool_si_no)
    prepared["_totale_non_confrontabile_bool"] = prepared[
        "totale_documento_puo_non_coincidere_con_mese_corrente"
    ].apply(parse_bool_si_no)

    for source_file, index in prepared.groupby("_source_file", sort=False).groups.items():
        source_group = prepared.loc[index]
        inferred_reconstructible = any(
            category in {"riga_analitica_mese", "tabella_supporto_consumi_mensili"}
            and str(row_manca).strip().lower() == "no"
            for category, row_manca in zip(
                source_group["_categoria_parser"].tolist(),
                source_group["manca_dettaglio"].fillna("").astype(str).tolist(),
            )
        )

        reconstruct_flags = [value for value in source_group["_dettaglio_ricostruzione_bool"].tolist() if value is not None]
        non_compare_flags = [value for value in source_group["_totale_non_confrontabile_bool"].tolist() if value is not None]

        final_reconstructible = any(reconstruct_flags) if reconstruct_flags else inferred_reconstructible
        source_has_cross_period_context = (
            len(
                {
                    (str(row.get("data_inizio", "")).strip(), str(row.get("data_fine", "")).strip())
                    for _, row in source_group.iterrows()
                    if str(row.get("data_inizio", "")).strip() or str(row.get("data_fine", "")).strip()
                }
            )
            > 1
            or bool(source_group["_presenza_ricalcolo_bool"].any())
            or bool(source_group["riferimento_ricalcolo_da"].fillna("").astype(str).str.strip().any())
            or bool(source_group["riferimento_ricalcolo_a"].fillna("").astype(str).str.strip().any())
        )
        final_non_comparable = any(non_compare_flags) if non_compare_flags else source_has_cross_period_context

        prepared.loc[index, "presenza_ricalcolo"] = prepared.loc[index, "_presenza_ricalcolo_bool"].map(bool_to_si_no)
        prepared.loc[index, "ricalcolo_aggregato_multi_mese"] = prepared.loc[index, "_ricalcolo_multi_bool"].map(
            bool_to_si_no
        )
        prepared.loc[index, "dettaglio_ricostruzione_presente"] = bool_to_si_no(final_reconstructible)
        prepared.loc[index, "totale_documento_puo_non_coincidere_con_mese_corrente"] = bool_to_si_no(
            final_non_comparable
        )
        prepared.loc[index, "_dettaglio_ricostruzione_bool"] = final_reconstructible
        prepared.loc[index, "_totale_non_confrontabile_bool"] = final_non_comparable

    return prepared


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    for column in CORE_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = ""

    prepared["_source_file"] = prepared["_source_file"].fillna("").astype(str)
    prepared["_tipo_componente_norm"] = prepared.apply(infer_tipo_componente, axis=1)
    prepared["_importo_num"] = prepared["importo"].apply(parse_number)
    prepared["_imponibile_mese_num"] = prepared["imponibile_mese"].apply(parse_number)
    prepared["_consumo_totale_num"] = prepared["consumo_totale"].apply(parse_number)
    prepared["_consumo_dettaglio_num"] = prepared["consumo_dettaglio_riga"].apply(parse_number)
    prepared["_quantita_num"] = prepared["quantita"].apply(parse_number)
    prepared["_unita_misura_norm"] = prepared["unita_misura"].fillna("").astype(str).str.strip().str.lower()
    prepared["_manca_dettaglio_bool"] = prepared["manca_dettaglio"].apply(parse_bool_si_no)
    prepared["_manca_dett_consumo_bool"] = prepared["manca_dettaglio_consumo"].apply(parse_bool_si_no)
    prepared["_data_fine_dt"] = prepared["data_fine"].apply(parse_date)
    prepared["_data_inizio_dt"] = prepared["data_inizio"].apply(parse_date)
    prepared["_rif_da_dt"] = prepared["riferimento_ricalcolo_da"].apply(parse_date)
    prepared["_rif_a_dt"] = prepared["riferimento_ricalcolo_a"].apply(parse_date)

    prepared["_period_dt"] = prepared["_data_fine_dt"]
    missing_period = prepared["_period_dt"].isna()
    prepared.loc[missing_period, "_period_dt"] = prepared.loc[missing_period, "_data_inizio_dt"]
    prepared = prepared.loc[prepared["_period_dt"].notna()].copy()

    prepared["_period_year"] = prepared["_period_dt"].dt.year.astype(int)
    prepared["_period_month"] = prepared["_period_dt"].dt.month.astype(int)
    prepared["_period_label"] = prepared.apply(
        lambda row: f"{int(row['_period_year']):04d}-{int(row['_period_month']):02d}",
        axis=1,
    )
    prepared = finalize_document_flags(prepared)
    prepared["_row_ricalcolo_multi_bool"] = prepared.apply(is_row_multi_month_recalc, axis=1)
    prepared["_row_is_aggregated_event"] = (
        prepared["_categoria_parser"].eq("totale_aggregato_multi_mese")
        | (
            prepared["_row_ricalcolo_multi_bool"]
            & prepared["_importo_num"].notna()
            & prepared["_presenza_ricalcolo_bool"]
        )
    )
    prepared["_warning_messages"] = [[] for _ in range(len(prepared))]
    prepared["_allocation_method"] = ""
    prepared["_allocation_month_count"] = 0
    prepared["_allocated_from_recalc"] = False
    prepared["_exclude_from_month_total"] = False
    prepared["_importo_alloc_num"] = prepared["_importo_num"]
    return prepared


def extract_group_warnings(group: pd.DataFrame) -> list[str]:
    warnings: list[str] = []
    for warning_list in group["_warning_messages"]:
        for warning in warning_list:
            if warning and warning not in warnings:
                warnings.append(warning)
    return warnings


def detect_duplicate_tariff_basis(detail_values: pd.Series, total_values: list[float]) -> bool:
    """Detect fake consumption detail repeated on amount rows as tariff basis."""
    if detail_values.empty or len(total_values) != 1 or len(detail_values) < 2:
        return False
    if detail_values.lt(0).any():
        return False

    rounded_details = {round(float(value), 6) for value in detail_values.tolist()}
    total_value = round(float(total_values[0]), 6)
    return len(rounded_details) == 1 and total_value in rounded_details


def classify_consumo_detail(detail_values: pd.Series, total_values: list[float]) -> tuple[str, float | None]:
    if detail_values.empty:
        return "none", None

    if detect_duplicate_tariff_basis(detail_values, total_values):
        return "duplicate_tariff_basis", None

    positives = detail_values[detail_values.gt(0)]
    negatives = detail_values[detail_values.lt(0)]

    if not positives.empty and not negatives.empty:
        return "signed_recalculation", float(detail_values.sum())

    if len(detail_values) == 1:
        return "single_consumption_row", float(detail_values.iloc[0])

    if negatives.empty and len(positives) > 1:
        return "multiple_positive_rows", None

    return "unclassified", None


def infer_month_quantity_from_rows(group: pd.DataFrame) -> float | None:
    candidate_rows = group.loc[
        group["_quantita_num"].notna()
        & group["_quantita_num"].gt(0)
        & group["_unita_misura_norm"].str.contains("smc|kwh|mc", regex=True)
        & ~group["_tipo_componente_norm"].eq("fissa")
    ].copy()
    if candidate_rows.empty:
        return None

    candidate_rows = candidate_rows.loc[
        ~candidate_rows["dettaglio_voce"].fillna("").astype(str).str.lower().str.contains("penale", regex=False)
    ].copy()
    if candidate_rows.empty:
        return None

    candidate_rows["_quantita_round"] = candidate_rows["_quantita_num"].round(6)
    distinct_values = candidate_rows["_quantita_round"].dropna().unique().tolist()
    counts = candidate_rows["_quantita_round"].value_counts()
    if counts.empty:
        return None

    top_value = float(counts.index[0])
    top_count = int(counts.iloc[0])
    second_count = int(counts.iloc[1]) if len(counts) > 1 else 0
    row_count = int(len(candidate_rows))

    # Avoid mistaking repeated step/scaglione quantities like 120/360/480 for the
    # real monthly consumption when the document already contains many different
    # quantity slices. In those cases the repeated value is just a tariff basis.
    dominant_repeated_value = (
        top_count >= 2
        and (
            len(distinct_values) <= 2
            or top_count / max(row_count, 1) >= 0.8
        )
    )
    if dominant_repeated_value:
        return top_value

    if len(distinct_values) == 1:
        return float(distinct_values[0])
    return None


def source_has_analytic_import_detail(source_group: pd.DataFrame) -> bool:
    rows = source_group.loc[
        source_group["_categoria_parser"].eq("riga_analitica_mese")
        & source_group["_importo_num"].notna()
        & ~source_group["_row_is_aggregated_event"]
    ]
    return not rows.empty


def source_has_consumo_support(source_group: pd.DataFrame) -> bool:
    if bool(source_group["_categoria_parser"].eq("tabella_supporto_consumi_mensili").any()):
        return True
    if infer_month_quantity_from_rows(source_group) is not None:
        return True
    detail_values = source_group["_consumo_dettaglio_num"].dropna()
    detail_pattern, _ = classify_consumo_detail(
        detail_values,
        unique_numbers_in_order(source_group["_consumo_totale_num"]),
    )
    return detail_pattern in {"signed_recalculation", "single_consumption_row"}


def classify_warning_reliability(warnings: list[str]) -> str:
    if not warnings:
        return "alta"

    severe_markers = (
        "verifica manuale consigliata",
        "manca una bolletta base completa",
        "periodo non e determinabile",
        "usato l'ultimo totale disponibile",
        "non bastano per ricostruire",
        "non allocato automaticamente",
        "importo non verificabile",
    )
    warning_text = " | ".join(warnings).lower()
    if any(marker in warning_text for marker in severe_markers):
        return "bassa"
    return "media"


def confidence_label_from_score(score: int) -> str:
    if score >= 90:
        return "alta"
    if score >= 65:
        return "media"
    return "bassa"


def compute_importo_confidence_profile(data: dict) -> dict[str, str | int]:
    importo_logic = str(data.get("importo_logica_usata", "") or "").strip()
    ricalcolo_presente = str(data.get("ricalcolo_presente", "")).strip().lower() == "si"
    ricalcolo_aggregato = str(data.get("ricalcolo_aggregato_multi_mese", "")).strip().lower() == "si"
    dettaglio_ricostruibile = str(data.get("dettaglio_ricostruzione_presente", "")).strip().lower() == "si"
    consumo_ricostruibile = str(data.get("consumo_mese_ricostruibile", "")).strip().lower() == "si"
    importo_ricostruibile = str(data.get("importo_mese_ricostruibile", "")).strip().lower() == "si"
    fallback_div2 = str(data.get("allocazione_fallback_dividi_per_due", "")).strip().lower() == "si"
    mismatch_senza_ricalcoli = (
        str(data.get("mismatch_dettaglio_vs_imponibile_senza_ricalcoli", "")).strip().lower() == "si"
    )
    manual_review = str(data.get("verifica_manuale_consigliata", "")).strip().lower() == "si"
    source_file_distinti_raw = data.get("source_file_distinti", "")
    try:
        source_file_distinti = (
            int(float(source_file_distinti_raw))
            if source_file_distinti_raw not in {"", None} and not pd.isna(source_file_distinti_raw)
            else 0
        )
    except (TypeError, ValueError):
        source_file_distinti = 0
    month_count_raw = data.get("mesi_coinvolti_ricalcolo_max", "")
    try:
        month_count = int(float(month_count_raw)) if month_count_raw not in {"", None} and not pd.isna(month_count_raw) else 0
    except (TypeError, ValueError):
        month_count = 0

    score = 100
    reason = "Imponibile/importo del mese coerente col documento."

    if importo_logic == "solo_ricalcoli_aggregati":
        score = 25
        reason = "Importo del mese non affidabile: presenti solo rettifiche successive senza bolletta base completa."
    elif ricalcolo_aggregato and consumo_ricostruibile and not importo_ricostruibile:
        score = 40
        reason = "Importo non verificabile con certezza: ricalcolo aggregato presente, ma il dettaglio economico del mese non basta."
    elif not ricalcolo_presente and mismatch_senza_ricalcoli:
        score = 94
        reason = "Imponibile affidabile dal documento, ma dettaglio economico non perfettamente coerente."
    elif ricalcolo_aggregato and dettaglio_ricostruibile:
        score = 82
        reason = "Ricalcolo aggregato presente, ma l'importo del mese resta ricostruibile dal dettaglio."
    elif ricalcolo_aggregato and fallback_div2:
        score = 58
        reason = "Importo ricostruito con fallback divide-per-due su ricalcolo aggregato."
    elif ricalcolo_aggregato and manual_review and month_count > 2:
        score = 30
        reason = "Importo del mese da verificare manualmente: ricalcolo aggregato su piu mesi senza dettaglio puntuale."
    elif ricalcolo_presente and not ricalcolo_aggregato and source_file_distinti > 1:
        score = 88
        reason = "Importo del mese ricostruibile, ma il mese e stato rettificato da una bolletta successiva."
    elif ricalcolo_presente:
        score = 88
        reason = "Importo del mese ricostruibile nonostante la presenza di ricalcoli."
    elif not importo_ricostruibile and consumo_ricostruibile:
        score = 72
        reason = "Consumo del mese disponibile, ma importo mensile non determinabile con certezza."
    elif "somma_importi" in importo_logic or importo_logic == "dettaglio_mese_con_ricalcolo":
        score = 90
        reason = "Importo del mese ricostruito direttamente dal dettaglio."
    elif importo_logic == "imponibile_documento":
        score = 100 if not mismatch_senza_ricalcoli else 94
        reason = (
            "Imponibile del documento coerente e usato come valore finale."
            if score == 100
            else "Imponibile del documento usato come valore finale; dettaglio economico da verificare."
        )

    score = max(0, min(100, int(score)))
    return {
        "importo_confidenza_percent": score,
        "importo_confidenza_motivo": reason,
        "importo_affidabilita": confidence_label_from_score(score),
    }


def compute_consumo_confidence_profile(data: dict) -> dict[str, str | int]:
    consumo_logic = str(data.get("consumo_logica_usata", "") or "").strip()
    ricalcolo_presente = str(data.get("ricalcolo_presente", "")).strip().lower() == "si"
    ricalcolo_aggregato = str(data.get("ricalcolo_aggregato_multi_mese", "")).strip().lower() == "si"
    consumo_ricostruibile = str(data.get("consumo_mese_ricostruibile", "")).strip().lower() == "si"
    manca_dettaglio_consumo = str(data.get("manca_dettaglio_consumo_mese", "")).strip().lower() == "si"
    consumo_dettaglio_righe_raw = data.get("consumo_dettaglio_righe", "")
    try:
        consumo_dettaglio_righe = (
            int(float(consumo_dettaglio_righe_raw))
            if consumo_dettaglio_righe_raw not in {"", None} and not pd.isna(consumo_dettaglio_righe_raw)
            else 0
        )
    except (TypeError, ValueError):
        consumo_dettaglio_righe = 0

    score = 100
    reason = "Consumo del mese coerente col documento."

    if consumo_logic == "non_determinato" or not consumo_ricostruibile:
        score = 25
        reason = "Consumo del mese non ricostruibile in modo affidabile."
    elif consumo_logic == "consumo_totale_ultimo_ricalcolo_mese":
        score = 50
        reason = "Consumo affidabile solo parzialmente: presente ricalcolo, usato il totale aggiornato piu recente del mese."
    elif consumo_logic == "somma_consumo_dettaglio_riga_ricalcolo":
        score = 50
        reason = "Consumo ricostruito da storno e nuovo valore del ricalcolo: verifica utile."
    elif consumo_logic == "consumo_totale_ultimo_documento":
        score = 45
        reason = "Consumo del mese stimato dall'ultimo totale disponibile: dettaglio di ricalcolo insufficiente."
    elif consumo_logic == "consumo_totale_ricalcolo_senza_dettaglio":
        score = 50
        reason = "Consumo del mese disponibile, ma il dettaglio del ricalcolo non basta a verificarlo."
    elif ricalcolo_aggregato and consumo_ricostruibile:
        score = 65
        reason = "Consumo mensile ricostruibile, ma il documento contiene un ricalcolo aggregato."
    elif consumo_logic == "quantita_righe_periodo":
        score = 92 if not ricalcolo_presente else 70
        reason = (
            "Consumo del mese ricostruito dalle quantita specifiche del periodo."
            if not ricalcolo_presente
            else "Consumo del mese ricostruito dalle quantita specifiche del periodo in presenza di ricalcolo."
        )
    elif consumo_logic == "consumo_totale":
        if not ricalcolo_presente:
            score = 100
            reason = "Consumo del mese coerente col totale del documento."
            if manca_dettaglio_consumo and consumo_dettaglio_righe == 0:
                reason = "Consumo del mese coerente col documento anche senza dettaglio consumo analitico."
        else:
            score = 70
            reason = "Consumo del mese preso dal totale documento, ma il mese risulta toccato da ricalcoli."
    elif consumo_logic == "consumo_dettaglio_singola_riga":
        score = 90
        reason = "Consumo del mese ricostruito da una riga specifica di consumo."

    score = max(0, min(100, int(score)))
    return {
        "consumo_confidenza_percent": score,
        "consumo_confidenza_motivo": reason,
        "consumo_affidabilita": confidence_label_from_score(score),
    }


def extract_allocated_month_count(warning_text: str) -> int | None:
    match = re.search(r"su\s+(\d+)\s+mesi", warning_text or "", re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def compute_confidence_profile(record: dict | pd.Series) -> dict[str, str | int]:
    data = record.to_dict() if hasattr(record, "to_dict") else dict(record)
    importo_profile = compute_importo_confidence_profile(data)
    consumo_profile = compute_consumo_confidence_profile(data)
    importo_score = int(importo_profile["importo_confidenza_percent"])
    consumo_score = int(consumo_profile["consumo_confidenza_percent"])
    score = int(round((importo_score + consumo_score) / 2))
    reason = (
        f"Imponibile/importo: {importo_profile['importo_confidenza_percent']}% - {importo_profile['importo_confidenza_motivo']} "
        f"| Consumo: {consumo_profile['consumo_confidenza_percent']}% - {consumo_profile['consumo_confidenza_motivo']}"
    )
    return {
        "confidenza_percent": score,
        "confidenza_motivo": reason,
        "affidabilita_mese": confidence_label_from_score(score),
        **importo_profile,
        **consumo_profile,
    }


def is_actionable_warning(message: str) -> bool:
    text = (message or "").strip().lower()
    if not text:
        return False
    markers = (
        "verifica manuale consigliata",
        "manca una bolletta base completa",
        "periodo non e determinabile",
        "usato l'ultimo totale disponibile",
        "consumo del mese corretto da ricalcolo",
        "consumo del mese ricostruito",
        "consumo mensile ricostruibile",
        "rettificato in una bolletta successiva",
        "presenti ricalcoli anche in altre bollette",
        "non bastano per ricostruire",
        "divide per due",
        "non va letto come valore del solo mese",
        "dettaglio e imponibile non coincidono",
        "non allocato automaticamente",
        "importo non verificabile",
    )
    return any(marker in text for marker in markers)


def compute_consumo_mese(group: pd.DataFrame) -> dict:
    base_group = group.loc[~group["_allocated_from_recalc"]].copy()
    detail_values = base_group["_consumo_dettaglio_num"].dropna()
    total_values = unique_numbers_in_order(base_group["_consumo_totale_num"])
    row_quantity_candidate = infer_month_quantity_from_rows(base_group)

    source_count = group["_source_file"].replace("", pd.NA).dropna().nunique()
    detail_mixed_sign = not detail_values.empty and (detail_values.lt(0).any() and detail_values.gt(0).any())
    recalc_present = bool(
        group["_presenza_ricalcolo_bool"].any()
        or group["_categoria_parser"].isin(
            {"evento_ricalcolo", "evento_storno", "evento_acconto_precedente", "totale_aggregato_multi_mese"}
        ).any()
        or group["_allocated_from_recalc"].any()
    )
    multi_month_recalc_present = bool(
        group["_allocated_from_recalc"].any()
        or group["_row_is_aggregated_event"].any()
        or group["_row_ricalcolo_multi_bool"].any()
    )
    mese_ricalcolato = (
        source_count > 1
        or len(total_values) > 1
        or detail_mixed_sign
        or recalc_present
    )

    consumo_flags = [flag for flag in group["_manca_dett_consumo_bool"].tolist() if flag is not None]
    detail_pattern, detail_value = classify_consumo_detail(detail_values, total_values)
    missing_detail_patterns = {"none", "duplicate_tariff_basis", "multiple_positive_rows", "unclassified"}
    manca_dettaglio_consumo_mese = (
        (bool(consumo_flags) and all(consumo_flags))
        or (bool(total_values) and detail_pattern in missing_detail_patterns)
    )

    warnings = extract_group_warnings(group)
    problem_patterns = {"duplicate_tariff_basis", "multiple_positive_rows", "unclassified"}
    has_support_table = bool(group["_categoria_parser"].eq("tabella_supporto_consumi_mensili").any())

    if recalc_present and not multi_month_recalc_present and total_values:
        consumo_mese = total_values[-1]
        logica = "consumo_totale_ultimo_ricalcolo_mese"
        warnings.append("Consumo del mese corretto da ricalcolo: usato il totale aggiornato piu recente del mese")
    elif len(total_values) == 1 and not recalc_present:
        if row_quantity_candidate is not None and abs(row_quantity_candidate - total_values[0]) > 0.5:
            consumo_mese = float(row_quantity_candidate)
            logica = "quantita_righe_periodo"
        else:
            consumo_mese = total_values[0]
            logica = "consumo_totale"
    elif detail_pattern == "signed_recalculation":
        consumo_mese = float(detail_value)
        logica = "somma_consumo_dettaglio_riga_ricalcolo"
        warnings.append("Consumo del mese ricostruito dal dettaglio di storno e nuovo valore")
    elif row_quantity_candidate is not None and (
        not total_values or any(abs(row_quantity_candidate - total_value) > 0.5 for total_value in total_values)
    ):
        consumo_mese = float(row_quantity_candidate)
        logica = "quantita_righe_periodo"
        if mese_ricalcolato or has_support_table:
            warnings.append("Consumo del mese ricostruito dalle quantita specifiche del periodo")
    elif detail_pattern == "single_consumption_row" and len(total_values) == 0:
        consumo_mese = float(detail_value)
        logica = "consumo_dettaglio_singola_riga"
        if mese_ricalcolato:
            warnings.append("Consumo del mese ricostruito da una riga di consumo specifica")
    elif len(total_values) == 1:
        consumo_mese = total_values[0]
        if mese_ricalcolato and detail_pattern != "signed_recalculation":
            logica = "consumo_totale_ricalcolo_senza_dettaglio"
            warnings.append(
                "Per il consumo non c'e dettaglio vecchio/nuovo sufficiente: usato il totale disponibile del documento"
            )
            if detail_pattern in problem_patterns:
                warnings.append(
                    "Le quantita presenti non bastano a ricostruire il consumo del ricalcolo: verifica manuale consigliata"
                )
        else:
            logica = "consumo_totale"
    elif total_values:
        consumo_mese = total_values[-1]
        logica = "consumo_totale_ultimo_documento"
        warnings.append("Per il consumo non c'e dettaglio vecchio/nuovo sufficiente: usato l'ultimo totale disponibile")
        if detail_pattern in problem_patterns:
            warnings.append(
                "Le quantita presenti non bastano a ricostruire il consumo del ricalcolo: verifica manuale consigliata"
            )
    else:
        consumo_mese = None
        logica = "non_determinato"
        warnings.append("Consumi non sufficienti per ricostruire il mese in modo affidabile")

    if mese_ricalcolato and detail_pattern == "signed_recalculation" and logica != "consumo_totale_ultimo_ricalcolo_mese":
        logica = "somma_consumo_dettaglio_riga_ricalcolo"

    consumo_ricostruibile = False
    if consumo_mese is not None:
        if (
            row_quantity_candidate is not None
            or detail_pattern in {"signed_recalculation", "single_consumption_row"}
            or has_support_table
        ):
            consumo_ricostruibile = True
        elif not mese_ricalcolato:
            consumo_ricostruibile = True
        elif len(total_values) == 1 and detail_pattern not in problem_patterns:
            consumo_ricostruibile = True

    if multi_month_recalc_present and consumo_ricostruibile and not any(
        "consumo mensile ricostruibile" in warning.lower() for warning in warnings
    ):
        warnings.append(
            "Consumo mensile ricostruibile, anche se il documento contiene ricalcoli o competenze su piu mesi"
        )

    return {
        "consumo_mese": consumo_mese,
        "consumo_logica_usata": logica,
        "consumo_mese_ricostruibile": "si" if consumo_ricostruibile else "no",
        "mese_ricalcolato": mese_ricalcolato,
        "source_file_distinti": int(source_count),
        "consumi_totali_distinti": int(len(total_values)),
        "manca_dettaglio_consumo_mese": "si" if manca_dettaglio_consumo_mese else "no",
        "consumo_dettaglio_righe": 0 if detail_pattern in {"duplicate_tariff_basis", "multiple_positive_rows"} else int(detail_values.notna().sum()),
        "consumo_dettaglio_sum": None if detail_pattern in {"duplicate_tariff_basis", "multiple_positive_rows"} or detail_values.empty else float(detail_values.sum()),
        "warning_messages": warnings,
    }


def compute_importo_mese(group: pd.DataFrame) -> dict:
    warnings = extract_group_warnings(group)
    warning_flags = empty_warning_flags()
    base_group = group.loc[~group["_allocated_from_recalc"]].copy()
    recalc_group = group.loc[group["_allocated_from_recalc"]].copy()
    month_has_any_recalc = bool(
        group["_presenza_ricalcolo_bool"].any()
        or group["_categoria_parser"].isin(
            {"evento_ricalcolo", "evento_storno", "evento_acconto_precedente", "totale_aggregato_multi_mese"}
        ).any()
        or group["_allocated_from_recalc"].any()
    )

    base_total = 0.0
    base_logic = "nessuna_base_mese"
    source_import_reconstructible: list[bool] = []

    if not base_group.empty:
        source_totals: list[float] = []
        source_logics: list[str] = []
        for _, source_group in base_group.groupby("_source_file", sort=False):
            imponibile_value = source_group["_imponibile_mese_num"].dropna().max()
            detail_sum = float(source_group["_importo_num"].fillna(0).sum())
            detail_count = int(source_group["_importo_num"].notna().sum())
            source_presence_flag = bool(source_group["_presenza_ricalcolo_bool"].any())
            source_agg_flag = bool(source_group["_ricalcolo_multi_bool"].any())
            source_presence_recalc = bool(
                source_presence_flag
                or
                source_group["_categoria_parser"].isin(
                    {"evento_ricalcolo", "evento_storno", "evento_acconto_precedente", "totale_aggregato_multi_mese"}
                ).any()
                or source_group["_allocated_from_recalc"].any()
            )
            source_agg_multi = bool(
                source_agg_flag
                or source_group["_row_ricalcolo_multi_bool"].any()
                or source_group["_allocated_from_recalc"].any()
            )
            source_reconstructible = bool(source_group["_dettaglio_ricostruzione_bool"].any())
            source_total_may_not_match = bool(source_group["_totale_non_confrontabile_bool"].any())
            source_has_import_detail = source_has_analytic_import_detail(source_group)
            source_has_consumo_only_support = source_has_consumo_support(source_group) and not source_has_import_detail

            if source_presence_recalc:
                mark_warning_flag(warning_flags, "ricalcolo_presente")
            if source_agg_multi:
                mark_warning_flag(warning_flags, "ricalcolo_aggregato_multi_mese")
            if source_reconstructible and source_agg_multi:
                mark_warning_flag(warning_flags, "dettaglio_ricostruzione_presente")
            if source_total_may_not_match:
                mark_warning_flag(
                    warning_flags,
                    "totale_documento_non_confrontabile_direttamente_con_mese_corrente",
                )

            if not source_presence_recalc and month_has_any_recalc and pd.notna(imponibile_value):
                source_totals.append(float(imponibile_value))
                source_logics.append("imponibile_documento_rettificato_in_bolletta_successiva")
                source_import_reconstructible.append(True)
                warnings.append(
                    "Per questo mese sono presenti ricalcoli anche in altre bollette: usato l'imponibile della bolletta base e sommate le rettifiche successive"
                )
            elif not source_presence_recalc and month_has_any_recalc and detail_count > 0:
                source_totals.append(detail_sum)
                source_logics.append("dettaglio_mese_per_presenza_ricalcoli_nel_mese")
                source_import_reconstructible.append(True)
                warnings.append(
                    "Per questo mese sono presenti ricalcoli anche in altre bollette: imponibile della bolletta base assente, usata la somma del dettaglio del mese"
                )
            elif not source_presence_recalc and pd.notna(imponibile_value):
                source_totals.append(float(imponibile_value))
                source_logics.append("imponibile_documento")
                source_import_reconstructible.append(True)
                mark_warning_flag(
                    warning_flags,
                    "nessun_ricalcolo_rilevato_imponibile_usato_come_fonte_finale",
                )
                if detail_count > 0 and abs(float(imponibile_value) - detail_sum) > DETAIL_VS_IMPONIBILE_TOLERANCE:
                    mark_warning_flag(
                        warning_flags,
                        "mismatch_dettaglio_vs_imponibile_senza_ricalcoli",
                    )
                    mark_warning_flag(warning_flags, "verifica_manuale_consigliata")
                    warnings.append(
                        "Caso standard senza ricalcoli: dettaglio e imponibile non coincidono, usato l'imponibile come valore finale"
                    )
                else:
                    warnings.append("Caso standard senza ricalcoli: usato l'imponibile del documento come valore finale")
            elif source_presence_recalc:
                source_totals.append(detail_sum)
                if source_agg_multi and source_reconstructible:
                    if source_has_import_detail and detail_count > 0:
                        source_logics.append("dettaglio_mese_con_ricalcolo_aggregato_ricostruibile")
                        source_import_reconstructible.append(True)
                        warnings.append(
                            "Ricalcolo presente, ma non mancano informazioni: importo mensile ricostruibile dal dettaglio"
                        )
                    else:
                        source_logics.append("supporto_consumo_con_ricalcolo_aggregato")
                        source_import_reconstructible.append(False)
                        warnings.append(
                            "Consumo mensile ricostruibile dal dettaglio, ma importo mensile non determinabile con certezza per presenza di ricalcolo aggregato"
                        )
                elif source_agg_multi:
                    source_logics.append("dettaglio_mese_con_ricalcolo_aggregato")
                    source_import_reconstructible.append(False)
                    warnings.append("Ricalcolo aggregato presente nel documento")
                else:
                    source_logics.append("dettaglio_mese_con_ricalcolo")
                    source_import_reconstructible.append(True)
                    if source_total_may_not_match:
                        warnings.append(
                            "Imponibile preso da somma del dettaglio del mese, in quanto nella bolletta erano presenti anche altri mesi come ricalcolo"
                        )
                    else:
                        warnings.append("Ricalcolo presente, ma non mancano informazioni: importo mensile ricostruibile")
                if source_total_may_not_match:
                    warnings.append("Il totale del documento include anche altre competenze: non va letto come valore del solo mese")
            elif pd.notna(imponibile_value):
                source_totals.append(float(imponibile_value))
                source_logics.append("imponibile_documento")
                source_import_reconstructible.append(True)
            else:
                if detail_count > 0:
                    source_totals.append(detail_sum)
                    source_logics.append("somma_importi")
                    source_import_reconstructible.append(not source_has_consumo_only_support)

        base_total = float(sum(source_totals))
        if source_logics and all(
            logic in {"imponibile_documento", "imponibile_documento_rettificato_in_bolletta_successiva"}
            for logic in source_logics
        ):
            base_logic = "imponibile_documento"
        elif (
            source_logics
            and "imponibile_documento_rettificato_in_bolletta_successiva" in source_logics
            and all(
                logic in {"imponibile_documento_rettificato_in_bolletta_successiva", "dettaglio_mese_con_ricalcolo"}
                for logic in source_logics
            )
        ):
            base_logic = "imponibile_documento_piu_rettifiche_nel_mese"
        elif source_logics and all(
            logic in {"dettaglio_mese_con_ricalcolo", "dettaglio_mese_per_presenza_ricalcoli_nel_mese"}
            for logic in source_logics
        ):
            base_logic = "dettaglio_mese_con_ricalcolo"
        elif source_logics and all(
            logic == "dettaglio_mese_con_ricalcolo_aggregato_ricostruibile" for logic in source_logics
        ):
            base_logic = "dettaglio_mese_con_ricalcolo_aggregato_ricostruibile"
        elif source_logics and all(logic == "somma_importi" for logic in source_logics):
            base_logic = "somma_importi"
        else:
            base_logic = "misto_base"

    recalc_total = float(recalc_group["_importo_alloc_num"].fillna(0).sum()) if not recalc_group.empty else 0.0
    totale = base_total + recalc_total
    importo_ricostruibile = bool(source_import_reconstructible) and all(source_import_reconstructible)
    recalc_methods = set(unique_nonempty_texts(recalc_group["_allocation_method"])) if not recalc_group.empty else set()
    if {
        "allocazione_proporzionale_consumi_importo_non_verificabile",
        "fallback_dividi_per_due",
    } & recalc_methods:
        importo_ricostruibile = False

    if recalc_group.empty:
        logica = base_logic if base_logic != "nessuna_base_mese" else "somma_importi_fallback"
    elif base_group.empty:
        logica = "solo_ricalcoli_aggregati"
        importo_ricostruibile = False
        warnings.append("Per questo mese sono presenti solo rettifiche successive: manca una bolletta base completa")
        mark_warning_flag(warning_flags, "verifica_manuale_consigliata")
    else:
        if base_logic == "imponibile_documento":
            logica = "imponibile_documento_piu_ricalcoli"
        elif base_logic == "somma_importi":
            logica = "somma_importi_piu_ricalcoli"
        else:
            logica = "misto_base_piu_ricalcoli"

    warnings = list(dict.fromkeys(warnings))

    return {
        "totale_importi": totale,
        "importo_logica_usata": logica,
        "importo_mese_ricostruibile": "si" if importo_ricostruibile else "no",
        "warning_messages": warnings,
        "warning_flags": warning_flags,
    }


def build_consumo_basis(df: pd.DataFrame) -> dict[tuple[int, int], float]:
    basis: dict[tuple[int, int], float] = {}
    normal_rows = df.loc[~df["_row_is_aggregated_event"]].copy()
    if normal_rows.empty:
        return basis

    for (year, month), group in normal_rows.groupby(["_period_year", "_period_month"], sort=True):
        support_rows = group.loc[group["_categoria_parser"].eq("tabella_supporto_consumi_mensili")]
        if not support_rows.empty:
            support_quantity = infer_month_quantity_from_rows(support_rows)
            if support_quantity is not None and support_quantity > 0:
                # Support-table rows may carry a document-level total that belongs to
                # a wider statement window; for proportional allocations we want the
                # month-specific quantity shown on the row itself.
                basis[(int(year), int(month))] = float(support_quantity)
                continue
        consumo_info = compute_consumo_mese(group)
        consumo_mese = consumo_info["consumo_mese"]
        if consumo_mese is not None and consumo_mese > 0:
            basis[(int(year), int(month))] = float(consumo_mese)
    return basis


def build_proportional_weights_from_consumi(
    target_months: list[pd.Timestamp],
    consumo_basis: dict[tuple[int, int], float],
) -> list[float] | None:
    if not target_months:
        return None

    weights_source: list[float] = []
    for month_ts in target_months:
        value = consumo_basis.get((int(month_ts.year), int(month_ts.month)))
        if value is None or value <= 0:
            return None
        weights_source.append(float(value))

    total = sum(weights_source)
    if total <= 0:
        return None
    return [value / total for value in weights_source]


def build_ricalcolo_distribution(
    row: pd.Series,
    target_months: list[pd.Timestamp],
    consumo_basis: dict[tuple[int, int], float],
    source_has_import_detail: bool = False,
    source_has_consumo_support: bool = False,
) -> tuple[list[float], str, list[str], dict[str, str]]:
    month_count = len(target_months)
    flags = empty_warning_flags()
    warnings: list[str] = []
    component = str(row.get("_tipo_componente_norm", "") or "").strip().lower()

    mark_warning_flag(flags, "ricalcolo_presente")
    mark_warning_flag(flags, "ricalcolo_aggregato_multi_mese")
    mark_warning_flag(flags, "totale_documento_non_confrontabile_direttamente_con_mese_corrente")

    if month_count == 0:
        mark_warning_flag(flags, "verifica_manuale_consigliata")
        warnings.append("Ricalcolo aggregato presente, ma il periodo non e determinabile con precisione: verifica manuale consigliata")
        return [], "non_allocato_intervallo_non_determinabile", warnings, flags

    if bool(row.get("_dettaglio_ricostruzione_bool")):
        mark_warning_flag(flags, "dettaglio_ricostruzione_presente")
        if source_has_import_detail or not source_has_consumo_support:
            warnings.append(
                "Ricalcolo aggregato presente, ma il mese e ricostruibile dal dettaglio: ignorato il totale aggregato"
            )
            return [], "dettaglio_ricostruibile_nessuna_allocazione", warnings, flags

    if month_count == 1:
        warnings.append("Ricalcolo riferito a un solo mese: importo mantenuto sul mese corretto")
        return [1.0], "allocazione_periodo_unico", warnings, flags

    proportional_weights = build_proportional_weights_from_consumi(target_months, consumo_basis)

    if component == "fissa":
        warnings.append(f"Ricalcolo aggregato con componente fissa: importo ripartito uniformemente su {month_count} mesi")
        return [1 / month_count] * month_count, "allocazione_uniforme_fissa", warnings, flags

    if component == "variabile" and proportional_weights is not None:
        warnings.append("Ricalcolo aggregato con componente variabile: importo ripartito in proporzione ai consumi mensili")
        return proportional_weights, "allocazione_proporzionale_consumi", warnings, flags

    if source_has_consumo_support and proportional_weights is not None:
        mark_warning_flag(flags, "verifica_manuale_consigliata")
        warnings.append(
            "Aggregazione presente ma dettaglio importi no, consumi si: consumo probabilmente corretto ma importo non verificabile"
        )
        return proportional_weights, "allocazione_proporzionale_consumi_importo_non_verificabile", warnings, flags

    if month_count == 2:
        mark_warning_flag(flags, "allocazione_fallback_dividi_per_due")
        mark_warning_flag(flags, "verifica_manuale_consigliata")
        warnings.append("Ricalcolo aggregato su due mesi senza dettaglio puntuale: applicato fallback divide-per-due")
        return [0.5, 0.5], "fallback_dividi_per_due", warnings, flags

    mark_warning_flag(flags, "verifica_manuale_consigliata")
    warnings.append("Ricalcolo aggregato su piu mesi senza dettaglio puntuale: non allocato automaticamente, verifica manuale consigliata")
    return [], "non_allocato_multi_mese_senza_dettaglio", warnings, flags


def expand_aggregated_recalculation_rows(
    df: pd.DataFrame,
    consumo_basis: dict[tuple[int, int], float],
) -> pd.DataFrame:
    expanded_rows: list[dict] = []

    for _, row in df.iterrows():
        base_record = row.to_dict()
        if not bool(row["_row_is_aggregated_event"]):
            expanded_rows.append(base_record)
            continue

        target_months = month_range(row["_rif_da_dt"], row["_rif_a_dt"])
        source_group = df.loc[df["_source_file"] == row["_source_file"]]
        weights, method, method_warnings, warning_flags = build_ricalcolo_distribution(
            row,
            target_months,
            consumo_basis,
            source_has_import_detail=source_has_analytic_import_detail(source_group),
            source_has_consumo_support=source_has_consumo_support(source_group),
        )

        importo_originale = row["_importo_num"] or 0.0
        if not weights:
            continue

        for month_ts, weight in zip(target_months, weights):
            allocated = dict(base_record)
            allocated["_period_year"] = int(month_ts.year)
            allocated["_period_month"] = int(month_ts.month)
            allocated["_period_label"] = f"{month_ts.year:04d}-{month_ts.month:02d}"
            allocated["_period_dt"] = month_ts
            allocated["_allocated_from_recalc"] = True
            allocated["_allocation_method"] = method
            allocated["_allocation_month_count"] = len(target_months)
            allocated["_importo_alloc_num"] = importo_originale * weight
            allocated["_consumo_totale_num"] = None
            allocated["_consumo_dettaglio_num"] = None
            allocated["_warning_messages"] = list(dict.fromkeys(base_record["_warning_messages"] + method_warnings))
            for flag_name, flag_value in warning_flags.items():
                allocated[flag_name] = flag_value

            expanded_rows.append(allocated)

    if not expanded_rows:
        return pd.DataFrame(columns=df.columns)
    return pd.DataFrame(expanded_rows)


def build_source_recalc_warning_report(
    prepared_df: pd.DataFrame,
) -> pd.DataFrame:
    if prepared_df.empty:
        return pd.DataFrame()

    rows: list[dict] = []
    recalc_rows = prepared_df.loc[prepared_df["_presenza_ricalcolo_bool"]].copy()
    if recalc_rows.empty:
        return pd.DataFrame()
    consumo_basis = build_consumo_basis(prepared_df)

    for _, row in recalc_rows.iterrows():
        warnings: list[str] = []
        warning_flags = empty_warning_flags()
        mark_warning_flag(warning_flags, "ricalcolo_presente")
        source_group = prepared_df.loc[prepared_df["_source_file"] == row["_source_file"]]
        source_has_import_detail = source_has_analytic_import_detail(source_group)
        source_has_consumo_only_support = source_has_consumo_support(source_group) and not source_has_import_detail
        source_has_reconstruction_support = (
            bool(row["_dettaglio_ricostruzione_bool"]) or source_has_import_detail or source_has_consumo_only_support
        )

        if bool(row["_totale_non_confrontabile_bool"]):
            mark_warning_flag(
                warning_flags,
                "totale_documento_non_confrontabile_direttamente_con_mese_corrente",
            )
            if bool(row["_row_is_aggregated_event"]):
                warnings.append("Il totale del documento include anche altre competenze: non va letto come valore del solo mese")
            else:
                warnings.append(
                    "La bolletta che rettifica questo mese include anche altre competenze: non usare il suo totale come valore del solo mese"
                )

        method = ""
        if bool(row["_row_is_aggregated_event"]):
            target_months = month_range(row["_rif_da_dt"], row["_rif_a_dt"])
            _, method, method_warnings, distribution_flags = build_ricalcolo_distribution(
                row,
                target_months,
                consumo_basis,
                source_has_import_detail=source_has_import_detail,
                source_has_consumo_support=source_has_consumo_support(source_group),
            )
            if source_has_consumo_only_support:
                method_warnings = [
                    (
                        "Consumo mensile ricostruibile dal dettaglio, ma importo mensile non determinabile con certezza per presenza di ricalcolo aggregato"
                        if "mese e ricostruibile dal dettaglio" in warning.lower()
                        else warning
                    )
                    for warning in method_warnings
                ]
            for warning in method_warnings:
                if warning not in warnings:
                    warnings.append(warning)
            warning_flags = merge_warning_flags(warning_flags, distribution_flags)
        else:
            target_months = []
            warnings.append("Questo mese e stato rettificato in una bolletta successiva")

        if bool(row["_dettaglio_ricostruzione_bool"]):
            mark_warning_flag(warning_flags, "dettaglio_ricostruzione_presente")
            if source_has_consumo_only_support:
                text = (
                    "Il documento contiene informazioni sufficienti per ricostruire almeno il consumo del mese, "
                    "ma non l'importo mensile con certezza"
                )
            else:
                text = "Il documento contiene informazioni sufficienti per ricostruire il mese"
            if text not in warnings:
                warnings.append(text)

        if bool(row["_row_is_aggregated_event"]):
            if source_has_consumo_only_support:
                mark_warning_flag(warning_flags, "verifica_manuale_consigliata")
                text = (
                    "Aggregazione presente ma dettaglio importi no, consumi si: "
                    "consumo probabilmente corretto ma importo non verificabile"
                )
                if text not in warnings:
                    warnings.append(text)
            elif not source_has_reconstruction_support:
                mark_warning_flag(warning_flags, "verifica_manuale_consigliata")
                warnings.append(
                    "Ricalcolo presente, ma le informazioni non bastano per ricostruire il mese: verifica manuale consigliata"
                )

        warnings = [warning for warning in warnings if warning]
        if not warnings:
            continue

        report_row = {
            "anno": int(row["_period_year"]),
            "mese": month_label(int(row["_period_year"]), int(row["_period_month"])),
            "affidabilita_mese": classify_warning_reliability(warnings),
            "warning_count": len(warnings),
            "warning_mese": " | ".join(dict.fromkeys(warnings)),
            "source_file_elenco": str(row["_source_file"]).strip(),
            "metodi_ripartizione_ricalcolo": method,
            "mesi_coinvolti_ricalcolo_max": len(target_months),
            "consumo_logica_usata": "",
            "importo_logica_usata": "",
        }
        report_row.update(warning_flags)
        rows.append(report_row)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def build_warning_report(aggregated_df: pd.DataFrame, source_warning_df: pd.DataFrame | None = None) -> pd.DataFrame:
    report_rows: list[dict] = []

    if aggregated_df is not None and not aggregated_df.empty and "warning_count" in aggregated_df.columns:
        for _, row in aggregated_df.iterrows():
            actionable = [warning for warning in split_warning_text(row.get("warning_mese", "")) if is_actionable_warning(warning)]
            if not actionable:
                continue
            report_rows.append(
                {
                    "anno": int(row["anno"]),
                    "mese": row["mese"],
                    "affidabilita_mese": classify_warning_reliability(actionable),
                    "warning_count": len(actionable),
                    "warning_mese": " | ".join(dict.fromkeys(actionable)),
                    "source_file_elenco": row.get("source_file_elenco", ""),
                    "metodi_ripartizione_ricalcolo": row.get("metodi_ripartizione_ricalcolo", ""),
                    "mesi_coinvolti_ricalcolo_max": row.get("mesi_coinvolti_ricalcolo_max", 0),
                    "consumo_logica_usata": row.get("consumo_logica_usata", ""),
                    "importo_logica_usata": row.get("importo_logica_usata", ""),
                    **{flag: row.get(flag, "no") for flag in WARNING_FLAG_COLUMNS},
                }
            )

    if source_warning_df is not None and not source_warning_df.empty:
        report_rows.extend(source_warning_df.to_dict(orient="records"))

    if not report_rows:
        return pd.DataFrame()

    report = pd.DataFrame(report_rows)
    grouped_rows: list[dict] = []
    for (anno, mese), group in report.groupby(["anno", "mese"], sort=True):
        warnings: list[str] = []
        for text in group["warning_mese"].fillna("").astype(str):
            for warning in split_warning_text(text):
                if warning not in warnings:
                    warnings.append(warning)

        source_files = unique_nonempty_texts(group["source_file_elenco"])
        methods = unique_nonempty_texts(group["metodi_ripartizione_ricalcolo"])
        consumo_logiche = unique_nonempty_texts(group["consumo_logica_usata"])
        importo_logiche = unique_nonempty_texts(group["importo_logica_usata"])
        month_counts = [
            int(float(value))
            for value in group.get("mesi_coinvolti_ricalcolo_max", pd.Series(dtype=float)).tolist()
            if str(value).strip() not in {"", "nan"}
        ]

        grouped_rows.append(
            {
                "anno": int(anno),
                "mese": mese,
                "affidabilita_mese": classify_warning_reliability(warnings),
                "warning_count": len(warnings),
                "warning_mese": " | ".join(warnings),
                "source_file_elenco": " | ".join(source_files),
                "metodi_ripartizione_ricalcolo": " | ".join(methods),
                "mesi_coinvolti_ricalcolo_max": max(month_counts) if month_counts else 0,
                "consumo_logica_usata": " | ".join(consumo_logiche),
                "importo_logica_usata": " | ".join(importo_logiche),
                **{
                    flag: "si"
                    if any(str(value).strip().lower() == "si" for value in group.get(flag, pd.Series(dtype=str)).tolist())
                    else "no"
                    for flag in WARNING_FLAG_COLUMNS
                },
            }
        )

    report = pd.DataFrame(grouped_rows).sort_values(["anno", "mese"]).reset_index(drop=True)
    if report.empty:
        return report

    confidence_df = report.apply(lambda row: pd.Series(compute_confidence_profile(row)), axis=1)
    for column in (
        "confidenza_percent",
        "confidenza_motivo",
        "affidabilita_mese",
        "importo_confidenza_percent",
        "importo_confidenza_motivo",
        "importo_affidabilita",
        "consumo_confidenza_percent",
        "consumo_confidenza_motivo",
        "consumo_affidabilita",
    ):
        report[column] = confidence_df[column]
    return report


def aggregate_bolletta_data(input_path, output_path) -> pd.DataFrame:
    input_path = Path(input_path)
    output_path = Path(output_path)

    df = read_table(input_path)
    if df.empty:
        empty = pd.DataFrame()
        with pd.ExcelWriter(output_path) as writer:
            empty.to_excel(writer, sheet_name="riepilogo_mesi", index=False)
        return empty

    prepared = prepare_dataframe(df)
    if prepared.empty:
        empty = pd.DataFrame()
        with pd.ExcelWriter(output_path) as writer:
            empty.to_excel(writer, sheet_name="riepilogo_mesi", index=False)
        return empty

    consumo_basis = build_consumo_basis(prepared)
    source_warning_report = build_source_recalc_warning_report(prepared)
    expanded = expand_aggregated_recalculation_rows(prepared, consumo_basis)

    monthly_rows: list[dict] = []
    for (year, month), group in expanded.groupby(["_period_year", "_period_month"], sort=True):
        consumo_info = compute_consumo_mese(group)
        importo_info = compute_importo_mese(group)

        warnings: list[str] = []
        for source_warnings in (extract_group_warnings(group), consumo_info["warning_messages"], importo_info["warning_messages"]):
            for warning in source_warnings:
                if warning and warning not in warnings:
                    warnings.append(warning)

        methods = unique_nonempty_texts(group.loc[group["_allocated_from_recalc"], "_allocation_method"])
        source_files = unique_nonempty_texts(group["_source_file"])
        month_warning_flags = merge_warning_flags(importo_info["warning_flags"])
        recalc_related_group = group.loc[
            group["_presenza_ricalcolo_bool"] | group["_allocated_from_recalc"] | group["_row_is_aggregated_event"]
        ]
        for flag_name in ("allocazione_fallback_dividi_per_due", "verifica_manuale_consigliata"):
            if flag_name in group.columns and any(str(value).strip().lower() == "si" for value in group[flag_name].fillna("").tolist()):
                mark_warning_flag(month_warning_flags, flag_name)
        if bool(group["_presenza_ricalcolo_bool"].any()):
            mark_warning_flag(month_warning_flags, "ricalcolo_presente")
        group_has_multi_recalc = bool(group["_row_ricalcolo_multi_bool"].any() or group["_allocated_from_recalc"].any())
        if group_has_multi_recalc:
            mark_warning_flag(month_warning_flags, "ricalcolo_aggregato_multi_mese")
        if bool(recalc_related_group["_dettaglio_ricostruzione_bool"].any()) and group_has_multi_recalc:
            mark_warning_flag(month_warning_flags, "dettaglio_ricostruzione_presente")
        if bool(group["_totale_non_confrontabile_bool"].any()):
            mark_warning_flag(
                month_warning_flags,
                "totale_documento_non_confrontabile_direttamente_con_mese_corrente",
            )
        if any("verifica manuale consigliata" in warning.lower() for warning in warnings):
            mark_warning_flag(month_warning_flags, "verifica_manuale_consigliata")

        monthly_rows.append(
            {
                "anno": int(year),
                "mese": month_label(int(year), int(month)),
                "mese_num": int(month),
                "totale_importi": importo_info["totale_importi"],
                "consumo_mese": consumo_info["consumo_mese"],
                "consumo_logica_usata": consumo_info["consumo_logica_usata"],
                "consumo_mese_ricostruibile": consumo_info["consumo_mese_ricostruibile"],
                "importo_logica_usata": importo_info["importo_logica_usata"],
                "importo_mese_ricostruibile": importo_info["importo_mese_ricostruibile"],
                "mese_ricalcolato": bool(consumo_info["mese_ricalcolato"] or group["_presenza_ricalcolo_bool"].any()),
                "source_file_distinti": int(len(source_files)),
                "source_file_elenco": " | ".join(source_files),
                "consumi_totali_distinti": int(consumo_info["consumi_totali_distinti"]),
                "manca_dettaglio_consumo_mese": consumo_info["manca_dettaglio_consumo_mese"],
                "consumo_dettaglio_righe": consumo_info["consumo_dettaglio_righe"],
                "consumo_dettaglio_sum": consumo_info["consumo_dettaglio_sum"],
                "ricalcolo_aggregato_presente": group_has_multi_recalc,
                "importo_ricalcoli_aggregati": float(
                    group.loc[group["_allocated_from_recalc"], "_importo_alloc_num"].fillna(0).sum()
                ),
                "metodi_ripartizione_ricalcolo": " | ".join(methods),
                "mesi_coinvolti_ricalcolo_max": int(
                    group.loc[group["_allocated_from_recalc"], "_allocation_month_count"].max()
                    if group["_allocated_from_recalc"].any()
                    else 0
                ),
                "manca_dettaglio_ricalcolo_mese": (
                    "si"
                    if group_has_multi_recalc and importo_info["importo_mese_ricostruibile"] == "no"
                    else "no"
                ),
                "affidabilita_mese": classify_warning_reliability(warnings),
                "warning_mese": " | ".join(warnings),
                "warning_count": len(warnings),
                **month_warning_flags,
            }
        )

    aggregated = pd.DataFrame(monthly_rows)
    if not aggregated.empty:
        aggregated = aggregated.sort_values(["anno", "mese_num"]).reset_index(drop=True)
    if not aggregated.empty:
        confidence_df = aggregated.apply(lambda row: pd.Series(compute_confidence_profile(row)), axis=1)
        for column in (
            "confidenza_percent",
            "confidenza_motivo",
            "affidabilita_mese",
            "importo_confidenza_percent",
            "importo_confidenza_motivo",
            "importo_affidabilita",
            "consumo_confidenza_percent",
            "consumo_confidenza_motivo",
            "consumo_affidabilita",
        ):
            aggregated[column] = confidence_df[column]
    warning_report = build_warning_report(aggregated, source_warning_report)
    aggregated.attrs["warning_report"] = warning_report

    with pd.ExcelWriter(output_path) as writer:
        aggregated.to_excel(writer, sheet_name="riepilogo_mesi", index=False)
        warning_report.to_excel(writer, sheet_name="warning_mesi", index=False)

    return aggregated


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    input_path, searched_paths = resolve_input_path(args.input_path)
    output_path = resolve_output_path(args.output_path)

    if not input_path.exists():
        searched = "\n".join(f"- {path}" for path in searched_paths)
        print(f"File input non trovato. Percorsi controllati:\n{searched}")
        return 1

    aggregate_bolletta_data(input_path, output_path)
    print(f"Output aggregato salvato in: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
