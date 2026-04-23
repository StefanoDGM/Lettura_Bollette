"""
Script Separato per Estrazione Dati da PDF Bollette

Questo script fa SOLO l'estrazione dei dati dai PDF.
Genera: estrazione_tutti_mesi.csv + .xlsx

Uso: python src/pipeline/process_bolletta.py [input_dir]
Es: python src/pipeline/process_bolletta.py data

Se non specificato, usa "data" nella root progetto e, se assente, ripiega su "tests/data".
"""

import argparse
import logging
import os
import re
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from pypdf import PdfReader

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root))
sys.path.insert(0, str(root / "src"))

from src.ai.gpt_client import MODEL_FALLBACK, MODEL_PRIMARY, call_gpt_with_pdf, review_gpt_with_pdf
from src.extractor.pdf_extractor import limit_pdf_pages

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

RESUME = False
RETRIES = 3
SLEEP_S = 0.5
DEFAULT_INPUT_DIR_CANDIDATES = (
    root / "data",
    root / "tests" / "data",
)
DECIMAL_EXPORT_COLUMNS = (
    "consumo_totale",
    "consumo_dettaglio_riga",
    "quantita",
    "prezzo_aliquota",
    "importo",
    "imponibile_mese",
)
EXPORT_PRIORITY_COLUMNS = (
    "_source_file",
    "nome_cliente",
    "pod",
    "pdr",
    "data_inizio",
    "data_fine",
    "consumo_totale",
    "consumo_dettaglio_riga",
    "tipo_componente",
    "riferimento_ricalcolo_da",
    "riferimento_ricalcolo_a",
    "presenza_ricalcolo",
    "ricalcolo_aggregato_multi_mese",
    "tipo_ricalcolo",
    "dettaglio_voce",
    "unita_misura",
    "quantita",
    "prezzo_aliquota",
    "importo",
    "imponibile_mese",
    "manca_dettaglio",
    "manca_dettaglio_consumo",
    "note",
)
HIDDEN_EXPORT_COLUMNS = {
    "blocco_ricalcolo_aggregato",
    "ricalcolo_spalmabile",
    "manca_dettaglio_ricalcolo",
    "dettaglio_ricostruzione_presente",
    "totale_documento_puo_non_coincidere_con_mese_corrente",
    "categoria_parser",
    "imponibile_mese_validato",
}
DETAIL_IMPONIBILE_TOLERANCE = Decimal("0.50")
FLAG_TRUE_VALUES = {"si", "sì", "yes", "true", "1"}
RICALCOLO_KEYWORDS = (
    "ricalcolo",
    "ricalcoli ex art",
    "importi gia contabilizzati",
    "importi già contabilizzati",
    "gia contabilizzati in bollette precedenti",
    "già contabilizzati in bollette precedenti",
    "fatture precedenti",
    "bollette precedenti",
)
STORNO_KEYWORDS = ("storno", "rettifica", "accredito")
ACCONTO_PRECEDENTE_KEYWORDS = ("acconto", "acconti")
CONSUMO_RICALCOLO_KEYWORDS = (
    "ricalcolo consum",
    "storno consum",
    "consumo rideterminato",
    "rettifica consum",
    "ricalcolo smc",
    "ricalcolo mc",
    "ricalcolo kwh",
)
SUPPORTO_CONSUMI_KEYWORDS = ("consumi relativi agli ultimi mesi", "consumi degli ultimi mesi")
VALID_CATEGORIE_PARSER = {
    "riga_analitica_mese",
    "evento_ricalcolo",
    "evento_storno",
    "evento_acconto_precedente",
    "totale_aggregato_multi_mese",
    "tabella_supporto_consumi_mensili",
}
VALID_TIPI_RICALCOLO = {"importo", "consumo", "importo_e_consumo"}
VAT_REPARSE_NOTE = "imponibile_pdf_initial_read_mismatch_reparsed"
ART15_NOTE = "art15_excluded_in_iva_summary"
SUMMARY_SECTION_MARKERS = (
    "sintesi degli importi fatturati gas naturale",
    "sintesi degli importi fatturati",
)


def ensure_imponibile_validation_fields(rows: list[dict]) -> list[dict]:
    return [dict(row) for row in rows]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Estrae dati strutturati da tutti i PDF presenti in una cartella."
    )
    parser.add_argument(
        "input_dir",
        nargs="?",
        help='Cartella contenente i PDF. Se omessa usa "data" nella root progetto, poi "tests/data".',
    )
    return parser


def resolve_input_dir(input_dir_arg: Optional[str]) -> tuple[Path, list[Path]]:
    if input_dir_arg:
        raw_path = Path(input_dir_arg).expanduser()
        if raw_path.is_absolute():
            return raw_path, [raw_path]

        candidate_paths = []
        for base_path in (Path.cwd(), root):
            candidate = (base_path / raw_path).resolve()
            if candidate not in candidate_paths:
                candidate_paths.append(candidate)

        for candidate in candidate_paths:
            if candidate.exists():
                return candidate, candidate_paths

        return candidate_paths[-1], candidate_paths

    for candidate in DEFAULT_INPUT_DIR_CANDIDATES:
        if candidate.exists():
            return candidate, list(DEFAULT_INPUT_DIR_CANDIDATES)

    return DEFAULT_INPUT_DIR_CANDIDATES[0], list(DEFAULT_INPUT_DIR_CANDIDATES)


def find_pdf_files(input_dir: Path, pattern: str) -> list[Path]:
    if pattern.lower() == "*.pdf":
        return sorted(
            path for path in input_dir.iterdir()
            if path.is_file() and path.suffix.lower() == ".pdf"
        )
    return sorted(input_dir.glob(pattern))


def extract_rows_from_pdf(pdf_path: Path, model: str, context_hint: str = "") -> list[dict]:
    pdf_to_use = limit_pdf_pages(pdf_path)
    return call_gpt_with_pdf(pdf_to_use, model, context_hint=context_hint)


def build_recent_run_context(rows: list[dict], max_docs: int = 6) -> str:
    if not rows:
        return ""

    ordered_files: list[str] = []
    grouped_rows: dict[str, list[dict]] = {}
    for row in rows:
        source_file = str(row.get("_source_file", "")).strip()
        if not source_file:
            continue
        if source_file not in grouped_rows:
            grouped_rows[source_file] = []
            ordered_files.append(source_file)
        grouped_rows[source_file].append(row)

    if not ordered_files:
        return ""

    lines: list[str] = []
    for source_file in ordered_files[-max_docs:]:
        source_rows = grouped_rows[source_file]
        periods = unique_nonempty_texts(
            pd.Series(
                [
                    f"{str(row.get('data_inizio', '')).strip()}->{str(row.get('data_fine', '')).strip()}".strip("->")
                    for row in source_rows
                    if str(row.get("data_inizio", "")).strip() or str(row.get("data_fine", "")).strip()
                ]
            )
        )
        consumi = unique_nonempty_texts(
            pd.Series(
                [
                    normalize_decimal_for_export(row.get("consumo_totale"))
                    for row in source_rows
                    if normalize_decimal_for_export(row.get("consumo_totale"))
                ]
            )
        )
        imponibili = unique_nonempty_texts(
            pd.Series(
                [
                    normalize_decimal_for_export(row.get("imponibile_mese"))
                    for row in source_rows
                    if normalize_decimal_for_export(row.get("imponibile_mese"))
                ]
            )
        )
        parts = [f"- {source_file}"]
        if periods:
            parts.append(f"periodi: {', '.join(periods[:3])}")
        if consumi:
            parts.append(f"consumi_totali osservati: {', '.join(consumi[:4])}")
        if imponibili:
            parts.append(f"imponibili osservati: {', '.join(imponibili[:4])}")
        lines.append(" | ".join(parts))
    return "\n".join(lines)


def detect_platform_alert_from_error(error: Exception | str) -> str | None:
    text = str(error or "").strip().lower()
    if not text:
        return None
    if any(marker in text for marker in ("insufficient_quota", "billing_hard_limit_reached", "quota exceeded", "insufficient quota")):
        return "OpenAI segnala credito o budget esaurito sul platform: alcune elaborazioni potrebbero fermarsi."
    if "rate limit" in text and "quota" in text:
        return "OpenAI segnala limite/quota del platform vicino o raggiunto: verifica budget e limiti dell'account."
    return None


def get_manual_platform_alert() -> str | None:
    warning = os.environ.get("OPENAI_PLATFORM_BUDGET_WARNING", "").strip()
    if warning:
        return warning
    status = os.environ.get("OPENAI_PLATFORM_BUDGET_STATUS", "").strip().lower()
    if status in {"low", "critical", "warning"}:
        return "Budget OpenAI segnalato come basso dall'ambiente di esecuzione: monitora il credito residuo sul platform."
    return None


def parse_decimal_for_audit(value) -> Decimal | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    if "." in text and "," in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    else:
        text = text.replace(",", ".")

    text = re.sub(r"[^0-9\.\-]", "", text)
    if text in ("", "-", ".", "-."):
        return None

    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def format_decimal_for_audit(value: Decimal) -> str:
    normalized = format(value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    if normalized in ("-0", "-0.0"):
        normalized = "0"
    return normalized


def extract_text_from_pdf(pdf_path: Path) -> str:
    try:
        reader = PdfReader(str(pdf_path))
    except Exception:
        return ""

    parts: list[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text:
            parts.append(text)
    return "\n".join(parts)


def extract_riepilogo_iva_section(pdf_text: str) -> str:
    if not pdf_text:
        return ""

    normalized = re.sub(r"\s+", " ", pdf_text.replace("\xa0", " ")).strip()
    lowered = normalized.lower()
    section_start = lowered.find("riepilogo imponibile e iva")
    if section_start == -1:
        section_start = lowered.find("riepilogo iva")
    if section_start == -1:
        section_start = lowered.find("riepilogo i.v.a")
    if section_start != -1:
        normalized = normalized[section_start : section_start + 1800]
    return normalized


def extract_vat_imponibile_components_from_text(pdf_text: str) -> list[Decimal]:
    normalized = extract_riepilogo_iva_section(pdf_text)
    if not normalized:
        return []

    components: list[Decimal] = []
    patterns = (
        r"iva\s+(?:vendite\s+)?(?:al\s+)?\d{1,2}\s*%[^0-9]{0,80}imponibile(?:\s+di\s+euro|\s+euro)?[^0-9]{0,20}([0-9][0-9\.,]*)",
        r"iva\s+(?:vendite\s+)?(?:al\s+)?\d{1,2}\s*%[^0-9]{0,20}su[^0-9]{0,20}([0-9][0-9\.,]*)",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, normalized, flags=re.IGNORECASE):
            value = parse_decimal_for_audit(match.group(1))
            if value is not None and value not in components:
                components.append(value)
    return components


def extract_vat_imponibile_candidates_from_text(pdf_text: str) -> list[Decimal]:
    normalized = extract_riepilogo_iva_section(pdf_text)
    if not normalized:
        return []

    candidates: list[Decimal] = []
    total_candidates: list[Decimal] = []
    vat_components = extract_vat_imponibile_components_from_text(pdf_text)

    patterns = (
        r"totale\s+imponibile[^0-9]{0,20}([0-9][0-9\.,]*)",
        r"totale\s+servizi\s+gas\s+naturale[^0-9]{0,20}([0-9][0-9\.,]*)",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, normalized, flags=re.IGNORECASE):
            value = parse_decimal_for_audit(match.group(1))
            if value is not None:
                total_candidates.append(value)

    lowered = normalized.lower()
    if "iva al 5" in lowered or "iva 5%" in lowered:
        for match in re.finditer(
            r"imponibile(?:\s+di\s+euro|\s+euro)?[^0-9]{0,20}([0-9][0-9\.,]*)",
            normalized,
            flags=re.IGNORECASE,
        ):
            value = parse_decimal_for_audit(match.group(1))
            if value is not None:
                total_candidates.append(value)

    for candidate in total_candidates:
        if candidate not in candidates:
            candidates.append(candidate)

    if len(vat_components) > 1:
        vat_components_sum = sum(vat_components, Decimal("0"))
        if vat_components_sum not in candidates:
            candidates.append(vat_components_sum)
    elif len(vat_components) == 1 and not candidates:
        candidates.append(vat_components[0])

    unique_candidates: list[Decimal] = []
    for candidate in candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)
    return unique_candidates


def extract_vat_imponibile_from_text(
    pdf_text: str,
    expected_detail_sum: Decimal | None = None,
) -> Decimal | None:
    candidates = extract_vat_imponibile_candidates_from_text(pdf_text)
    if not candidates:
        return None
    if expected_detail_sum is None:
        return candidates[0]
    return min(candidates, key=lambda candidate: abs(candidate - expected_detail_sum))


def extract_vat_imponibile_from_pdf(
    pdf_path: Path,
    expected_detail_sum: Decimal | None = None,
) -> Decimal | None:
    return extract_vat_imponibile_from_text(extract_text_from_pdf(pdf_path), expected_detail_sum)


def extract_summary_amount_from_text(pdf_text: str, label: str) -> Decimal | None:
    if not pdf_text:
        return None

    normalized = re.sub(r"\s+", " ", pdf_text.replace("\xa0", " ")).strip()
    lowered = normalized.lower()
    section = normalized
    for marker in SUMMARY_SECTION_MARKERS:
        start = lowered.find(marker)
        if start != -1:
            end_candidates = [
                lowered.find("riepilogo imponibile e iva", start),
                lowered.find("riepilogo iva", start),
                lowered.find("totale fattura", start),
            ]
            end_candidates = [value for value in end_candidates if value != -1]
            end = min(end_candidates) if end_candidates else start + 1800
            section = normalized[start:end]
            break

    pattern = rf"{re.escape(label.lower())}[^0-9]{{0,40}}([0-9][0-9\.,]*)"
    match = re.search(pattern, section.lower(), flags=re.IGNORECASE)
    if not match:
        return None
    return parse_decimal_for_audit(match.group(1))


def select_primary_period_for_summary(rows: list[dict]) -> tuple[str, str] | None:
    grouped = group_rows_by_period(rows)
    best_key: tuple[str, str] | None = None
    best_score = (-1, Decimal("-1"))
    for key, group_rows in grouped.items():
        summary = summarize_group_imports(group_rows)
        score = (int(summary["base_count"]), summary["base_sum"] or Decimal("0"))
        if score > best_score:
            best_key = key
            best_score = score
    return best_key


def supplement_summary_macro_rows(
    pdf_path: Path,
    rows: list[dict],
    pdf_text: str | None = None,
) -> list[dict]:
    if not rows:
        return rows

    pdf_text = pdf_text if pdf_text is not None else extract_text_from_pdf(pdf_path)
    altre_partite = extract_summary_amount_from_text(pdf_text, "Altre partite")
    if altre_partite is None or altre_partite <= 0:
        return rows

    if any("altre partite" in str(row.get("dettaglio_voce", "")).strip().lower() for row in rows):
        return rows

    primary_period = select_primary_period_for_summary(rows)
    if primary_period is None:
        return rows

    period_rows = [
        row for row in rows
        if (str(row.get("data_inizio", "")).strip(), str(row.get("data_fine", "")).strip()) == primary_period
    ]
    template_row = next((row for row in period_rows if not is_explicit_recalculation_row(row)), None)
    if template_row is None:
        return rows

    amount_text = format_decimal_for_audit(altre_partite)
    note_text = "Macro-voce `Altre partite` aggiunta dal riepilogo economico del documento"
    new_row = dict(template_row)
    new_row["dettaglio_voce"] = "Altre partite"
    new_row["tipo_componente"] = "altro"
    new_row["importo"] = amount_text
    new_row["quantita"] = ""
    new_row["prezzo_aliquota"] = ""
    new_row["consumo_dettaglio_riga"] = ""
    note = str(new_row.get("note", "")).strip()
    new_row["note"] = f"{note} | {note_text}".strip(" |")
    rows.append(new_row)
    logging.info("[AUDIT] %s: aggiunta macro-voce mancante `Altre partite` = %s", pdf_path.name, amount_text)
    return rows


def normalize_detail_flags_from_rows(rows: list[dict]) -> list[dict]:
    grouped = group_rows_by_period(rows)
    reconstructible_keys: set[tuple[str, str]] = set()
    for key, group_rows in grouped.items():
        summary = summarize_group_imports(group_rows)
        only_explicit_recalc = int(summary["full_count"]) > 0 and int(summary["explicit_recalc_count"]) == int(summary["full_count"])
        if int(summary["base_count"]) >= 3 or only_explicit_recalc:
            reconstructible_keys.add(key)

    adjusted_rows: list[dict] = []
    for row in rows:
        adjusted = dict(row)
        key = (str(adjusted.get("data_inizio", "")).strip(), str(adjusted.get("data_fine", "")).strip())
        if key in reconstructible_keys:
            adjusted["manca_dettaglio"] = "no"
        adjusted_rows.append(adjusted)
    return adjusted_rows


def collect_document_periods(rows: list[dict]) -> set[tuple[str, str]]:
    periods = set()
    for row in rows:
        data_inizio = str(row.get("data_inizio", "")).strip()
        data_fine = str(row.get("data_fine", "")).strip()
        if data_inizio or data_fine:
            periods.add((data_inizio, data_fine))
    return periods


def unique_nonempty_texts(values) -> list[str]:
    unique_values: list[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in unique_values:
            unique_values.append(text)
    return unique_values


def normalize_si_no_flag(value, default: str = "") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return default
    return "si" if text in FLAG_TRUE_VALUES else "no"


def normalized_row_text(row: dict) -> str:
    parts = [
        str(row.get("dettaglio_voce", "")).strip().lower(),
        str(row.get("note", "")).strip().lower(),
    ]
    return " | ".join(part for part in parts if part)


def normalize_tipo_ricalcolo(value, default: str = "") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return default
    if text in VALID_TIPI_RICALCOLO:
        return text
    return default


def row_has_consumo_recalc_signal(row: dict) -> bool:
    if parse_decimal_for_audit(row.get("consumo_dettaglio_riga")) is not None:
        return True

    text = normalized_row_text(row)
    if any(keyword in text for keyword in CONSUMO_RICALCOLO_KEYWORDS):
        return True

    quantity = parse_decimal_for_audit(row.get("quantita"))
    unit = str(row.get("unita_misura", "")).strip().lower()
    dettaglio_consumo = normalize_si_no_flag(row.get("manca_dettaglio_consumo"))
    if quantity is not None and ("smc" in unit or "kwh" in unit or unit == "mc"):
        if quantity < 0:
            return True
        if dettaglio_consumo == "no":
            return True

    return False


def parse_date_text(value: str) -> pd.Timestamp | None:
    if not value:
        return pd.NaT
    return pd.to_datetime(str(value).strip(), dayfirst=True, errors="coerce")


def spans_multiple_months(data_inizio: str, data_fine: str) -> bool:
    start_dt = parse_date_text(data_inizio)
    end_dt = parse_date_text(data_fine)
    if pd.isna(start_dt) or pd.isna(end_dt):
        return False
    return (start_dt.year, start_dt.month) != (end_dt.year, end_dt.month)


def row_has_true_multi_month_scope(row: dict) -> bool:
    rif_da = str(row.get("riferimento_ricalcolo_da", "")).strip()
    rif_a = str(row.get("riferimento_ricalcolo_a", "")).strip()
    return bool(rif_da and rif_a and spans_multiple_months(rif_da, rif_a))


def group_rows_by_period(rows: list[dict]) -> dict[tuple[str, str], list[dict]]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        key = (str(row.get("data_inizio", "")).strip(), str(row.get("data_fine", "")).strip())
        grouped.setdefault(key, []).append(row)
    return grouped


def is_explicit_recalculation_row(row: dict) -> bool:
    if str(row.get("riferimento_ricalcolo_da", "")).strip() or str(row.get("riferimento_ricalcolo_a", "")).strip():
        return True
    if normalize_tipo_ricalcolo(row.get("tipo_ricalcolo")):
        return True
    if normalize_si_no_flag(row.get("presenza_ricalcolo")) == "si":
        text = normalized_row_text(row)
        if any(keyword in text for keyword in RICALCOLO_KEYWORDS + STORNO_KEYWORDS + ACCONTO_PRECEDENTE_KEYWORDS):
            return True
    return False


def infer_tipo_ricalcolo(row: dict, category: str = "") -> str:
    recalc_category = str(category or row.get("categoria_parser", "")).strip().lower()
    if recalc_category not in {
        "evento_ricalcolo",
        "evento_storno",
        "evento_acconto_precedente",
        "totale_aggregato_multi_mese",
    }:
        return ""

    raw_value = normalize_tipo_ricalcolo(row.get("tipo_ricalcolo"))
    if raw_value:
        return raw_value

    has_consumo_signal = row_has_consumo_recalc_signal(row)
    has_import_signal = parse_decimal_for_audit(row.get("importo")) is not None

    if has_consumo_signal and has_import_signal:
        return "importo_e_consumo"
    if has_consumo_signal:
        return "consumo"
    return "importo"


def derive_categoria_parser(row: dict) -> str:
    raw_category = str(row.get("categoria_parser", "")).strip().lower()
    text = normalized_row_text(row)
    data_inizio = str(row.get("data_inizio", "")).strip()
    data_fine = str(row.get("data_fine", "")).strip()
    rif_da = str(row.get("riferimento_ricalcolo_da", "")).strip()
    rif_a = str(row.get("riferimento_ricalcolo_a", "")).strip()

    if raw_category == "tabella_supporto_consumi_mensili":
        return raw_category

    if any(keyword in text for keyword in SUPPORTO_CONSUMI_KEYWORDS) and not str(row.get("importo", "")).strip():
        return "tabella_supporto_consumi_mensili"

    if row_has_true_multi_month_scope(row):
        return "totale_aggregato_multi_mese"

    if raw_category in VALID_CATEGORIE_PARSER and raw_category != "totale_aggregato_multi_mese":
        return raw_category

    if any(keyword in text for keyword in ACCONTO_PRECEDENTE_KEYWORDS) and any(
        keyword in text for keyword in ("precedenti", "precedente")
    ):
        return "evento_acconto_precedente"

    if any(keyword in text for keyword in STORNO_KEYWORDS):
        return "evento_storno"

    if any(keyword in text for keyword in RICALCOLO_KEYWORDS) or rif_da or rif_a:
        return "evento_ricalcolo"

    if data_inizio and data_fine:
        return "riga_analitica_mese"

    return "riga_analitica_mese"


def enrich_extracted_rows(rows: list[dict]) -> list[dict]:
    if not rows:
        return rows

    categories = [derive_categoria_parser(row) for row in rows]
    has_reconstructible_detail = any(
        category in {"riga_analitica_mese", "tabella_supporto_consumi_mensili"}
        and str(row.get("manca_dettaglio", "")).strip().lower() == "no"
        for category, row in zip(categories, rows)
    )
    has_cross_period_context = len(collect_document_periods(rows)) > 1 or any(
        category in {
            "evento_ricalcolo",
            "evento_storno",
            "evento_acconto_precedente",
            "totale_aggregato_multi_mese",
        }
        for category in categories
    )

    enriched_rows: list[dict] = []
    for category, row in zip(categories, rows):
        enriched = dict(row)
        enriched["categoria_parser"] = category
        row_has_recalculation = category in {
            "evento_ricalcolo",
            "evento_storno",
            "evento_acconto_precedente",
            "totale_aggregato_multi_mese",
        }
        row_has_multi_month_aggregate = category == "totale_aggregato_multi_mese"
        enriched["presenza_ricalcolo"] = "si" if row_has_recalculation else "no"
        enriched["ricalcolo_aggregato_multi_mese"] = "si" if row_has_multi_month_aggregate else "no"
        enriched["tipo_ricalcolo"] = infer_tipo_ricalcolo(enriched, category)
        enriched["dettaglio_ricostruzione_presente"] = normalize_si_no_flag(
            enriched.get("dettaglio_ricostruzione_presente"),
            "si" if has_reconstructible_detail else "no",
        )
        enriched["totale_documento_puo_non_coincidere_con_mese_corrente"] = normalize_si_no_flag(
            enriched.get("totale_documento_puo_non_coincidere_con_mese_corrente"),
            "si" if has_cross_period_context else "no",
        )
        enriched_rows.append(enriched)

    return enriched_rows


def summarize_group_imports(group_rows: list[dict]) -> dict[str, Decimal | int | None]:
    full_imports: list[Decimal] = []
    base_imports: list[Decimal] = []
    explicit_recalc_count = 0

    for row in group_rows:
        value = parse_decimal_for_audit(row.get("importo"))
        if value is None:
            continue
        full_imports.append(value)
        if is_explicit_recalculation_row(row):
            explicit_recalc_count += 1
        else:
            base_imports.append(value)

    return {
        "full_sum": sum(full_imports, Decimal("0")),
        "full_count": len(full_imports),
        "base_sum": sum(base_imports, Decimal("0")) if base_imports else None,
        "base_count": len(base_imports),
        "explicit_recalc_count": explicit_recalc_count,
    }


def document_has_cross_period_context(rows: list[dict]) -> bool:
    if len(collect_document_periods(rows)) > 1:
        return True
    for row in rows:
        if normalize_si_no_flag(row.get("presenza_ricalcolo")) == "si":
            return True
        if str(row.get("riferimento_ricalcolo_da", "")).strip() or str(row.get("riferimento_ricalcolo_a", "")).strip():
            return True
    return False


def is_standard_single_month_document(rows: list[dict]) -> bool:
    periods = collect_document_periods(rows)
    if len(periods) != 1:
        return False
    return not document_has_cross_period_context(rows)


def reconcile_standard_month_with_vat_summary(
    pdf_path: Path,
    rows: list[dict],
    vat_reader: Optional[Callable[[Path], Decimal | None]] = None,
) -> list[dict]:
    if not rows or not is_standard_single_month_document(rows):
        return ensure_imponibile_validation_fields(rows)

    rows = ensure_imponibile_validation_fields(rows)
    grouped = group_rows_by_period(rows)
    if len(grouped) != 1:
        return rows

    group_rows = next(iter(grouped.values()))
    group_summary = summarize_group_imports(group_rows)
    detail_sum = group_summary["full_sum"] if int(group_summary["full_count"]) > 0 else None

    pdf_text = extract_text_from_pdf(pdf_path)
    riepilogo_text = extract_riepilogo_iva_section(pdf_text)
    art15_present = bool(re.search(r"art\.?\s*15", riepilogo_text.lower()))
    vat_imponibile = (vat_reader or extract_vat_imponibile_from_pdf)(pdf_path)
    if vat_imponibile is None:
        return rows
    if detail_sum is None:
        return rows

    issue_key = next(iter(grouped.keys()))
    current_values = [
        parse_decimal_for_audit(row.get("imponibile_mese"))
        for row in rows
        if (str(row.get("data_inizio", "")).strip(), str(row.get("data_fine", "")).strip()) == issue_key
    ]
    current_values = [value for value in current_values if value is not None]
    current_imponibile = current_values[0] if current_values else None
    reparsed = False
    final_vat_imponibile = vat_imponibile

    if detail_sum is not None and vat_imponibile is not None and abs(vat_imponibile - detail_sum) > DETAIL_IMPONIBILE_TOLERANCE:
        reparsed = True
        reparsed_candidate = extract_vat_imponibile_from_pdf(pdf_path, detail_sum)
        if reparsed_candidate is not None:
            final_vat_imponibile = reparsed_candidate
    if final_vat_imponibile is None:
        return rows

    if current_imponibile is not None and abs(current_imponibile - final_vat_imponibile) <= DETAIL_IMPONIBILE_TOLERANCE:
        if not art15_present:
            return rows

        adjusted_rows: list[dict] = []
        for row in rows:
            adjusted = dict(row)
            if (str(adjusted.get("data_inizio", "")).strip(), str(adjusted.get("data_fine", "")).strip()) == issue_key:
                note = str(adjusted.get("note", "")).strip()
                if ART15_NOTE not in note:
                    adjusted["note"] = f"{note} | {ART15_NOTE}".strip(" |")
            adjusted_rows.append(adjusted)
        return adjusted_rows

    vat_text = format_decimal_for_audit(final_vat_imponibile)
    adjusted_rows: list[dict] = []
    for row in rows:
        adjusted = dict(row)
        if (str(adjusted.get("data_inizio", "")).strip(), str(adjusted.get("data_fine", "")).strip()) == issue_key:
            adjusted["imponibile_mese"] = vat_text
            adjusted["manca_dettaglio"] = "no"
            note = str(adjusted.get("note", "")).strip()
            if reparsed:
                note_text = (
                    f"{VAT_REPARSE_NOTE}: imponibile del Riepilogo IVA riletto dopo mismatch iniziale e riallineato al valore coerente del mese"
                )
            else:
                note_text = (
                    f"{VAT_REPARSE_NOTE}: imponibile riallineato dal Riepilogo IVA del PDF dopo prima lettura incoerente"
                )
            if note_text not in note:
                adjusted["note"] = f"{note} | {note_text}".strip(" |")
            if art15_present and ART15_NOTE not in adjusted["note"]:
                adjusted["note"] = f"{adjusted['note']} | {ART15_NOTE}".strip(" |")
        adjusted_rows.append(adjusted)
    return adjusted_rows


def find_detail_imponibile_issues(rows: list[dict], tolerance: Decimal = DETAIL_IMPONIBILE_TOLERANCE) -> list[dict]:
    rows = ensure_imponibile_validation_fields(rows)
    grouped = group_rows_by_period(rows)
    has_cross_periods = document_has_cross_period_context(rows)
    issues: list[dict] = []

    for (data_inizio, data_fine), group_rows in grouped.items():
        detail_flags = {
            str(row.get("manca_dettaglio", "")).strip().lower()
            for row in group_rows
            if str(row.get("manca_dettaglio", "")).strip()
        }
        if detail_flags != {"no"}:
            continue

        group_summary = summarize_group_imports(group_rows)
        if int(group_summary["full_count"]) == 0:
            continue

        imponibile_values = [parse_decimal_for_audit(row.get("imponibile_mese")) for row in group_rows]
        imponibile_values = [value for value in imponibile_values if value is not None]
        if not imponibile_values:
            continue

        row_sum = group_summary["full_sum"]
        reference = imponibile_values[0]
        delta = reference - row_sum
        if abs(delta) <= tolerance:
            continue

        if any(ART15_NOTE in str(row.get("note", "")).lower() for row in group_rows):
            continue

        preferred_sum = row_sum
        preferred_source = "full_sum"
        base_sum = group_summary["base_sum"]
        group_has_recalculation = any(
            normalize_si_no_flag(row.get("presenza_ricalcolo")) == "si" or is_explicit_recalculation_row(row)
            for row in group_rows
        )
        if (
            has_cross_periods
            and base_sum is not None
            and int(group_summary["base_count"]) >= 3
            and abs(reference - base_sum) < abs(delta)
        ):
            preferred_sum = base_sum
            preferred_source = "base_sum_without_explicit_recalc"

        if group_has_recalculation and preferred_source == "full_sum":
            continue

        issues.append(
            {
                "data_inizio": data_inizio,
                "data_fine": data_fine,
                "sum_importo": format_decimal_for_audit(row_sum),
                "preferred_detail_sum": format_decimal_for_audit(preferred_sum),
                "preferred_detail_source": preferred_source,
                "imponibile_mese": format_decimal_for_audit(reference),
                "delta": format_decimal_for_audit(delta),
                "cross_period_context": "si" if has_cross_periods else "no",
                "explicit_recalc_rows": int(group_summary["explicit_recalc_count"]),
                "presenza_ricalcolo": "si" if group_has_recalculation else "no",
            }
        )

    return issues


def mismatch_issue_score(issues: list[dict]) -> Decimal:
    total = Decimal("0")
    for issue in issues:
        value = parse_decimal_for_audit(issue.get("delta"))
        if value is not None:
            total += abs(value)
    return total


def mark_issues_as_missing_detail(rows: list[dict], issues: list[dict]) -> list[dict]:
    issue_keys = {
        (str(issue.get("data_inizio", "")).strip(), str(issue.get("data_fine", "")).strip()): str(issue.get("imponibile_mese", "")).strip()
        for issue in issues
    }
    note_text = "Dettaglio non coerente con imponibile del periodo dopo seconda verifica: usato imponibile_mese"

    adjusted_rows: list[dict] = []
    for row in rows:
        key = (str(row.get("data_inizio", "")).strip(), str(row.get("data_fine", "")).strip())
        adjusted = dict(row)
        if key in issue_keys:
            adjusted["manca_dettaglio"] = "si"
            if issue_keys[key]:
                adjusted["imponibile_mese"] = issue_keys[key]
            note = str(adjusted.get("note", "")).strip()
            if note_text not in note:
                adjusted["note"] = f"{note} | {note_text}".strip(" |")
        adjusted_rows.append(adjusted)
    return adjusted_rows


def should_prefer_detail_over_imponibile(group_rows: list[dict], issue: dict) -> bool:
    if str(issue.get("presenza_ricalcolo", "")).strip().lower() != "si":
        return False

    summary = summarize_group_imports(group_rows)
    for row in group_rows:
        if str(row.get("manca_dettaglio", "")).strip().lower() != "no":
            return False

    preferred_source = str(issue.get("preferred_detail_source", "")).strip()
    return (
        preferred_source == "base_sum_without_explicit_recalc"
        and int(summary["explicit_recalc_count"]) > 0
        and int(summary["base_count"]) >= 3
    )


def apply_detail_sum_override(rows: list[dict], issues: list[dict]) -> list[dict]:
    issue_map = {
        (str(issue.get("data_inizio", "")).strip(), str(issue.get("data_fine", "")).strip()): issue
        for issue in issues
    }
    adjusted_rows: list[dict] = []
    for row in rows:
        key = (str(row.get("data_inizio", "")).strip(), str(row.get("data_fine", "")).strip())
        adjusted = dict(row)
        issue = issue_map.get(key)
        if issue:
            preferred_source = str(issue.get("preferred_detail_source", "")).strip()
            note = str(adjusted.get("note", "")).strip()

            if preferred_source == "base_sum_without_explicit_recalc" and is_explicit_recalculation_row(adjusted):
                ref_da = str(adjusted.get("riferimento_ricalcolo_da", "")).strip()
                ref_a = str(adjusted.get("riferimento_ricalcolo_a", "")).strip()
                if ref_da:
                    adjusted["data_inizio"] = ref_da
                if ref_a:
                    adjusted["data_fine"] = ref_a
                relocation_note = "Riga di ricalcolo esclusa dal dettaglio del mese e riallocata al proprio periodo di riferimento"
                if relocation_note not in note:
                    adjusted["note"] = f"{note} | {relocation_note}".strip(" |")
            elif preferred_source == "base_sum_without_explicit_recalc":
                note_text = "Dettaglio del mese mantenuto separando le righe di ricalcolo esplicite per il post-processing finale"
                if note_text not in note:
                    adjusted["note"] = f"{note} | {note_text}".strip(" |")
        adjusted_rows.append(adjusted)
    return adjusted_rows


def recheck_rows_if_needed(
    pdf_path: Path,
    rows: list[dict],
    model: str,
    context_hint: str = "",
    review_callback: Optional[Callable[[Path, str, list[dict]], list[dict]]] = None,
) -> list[dict]:
    if review_callback is None and callable(context_hint):
        review_callback = context_hint  # backward compatibility with old call sites/tests
        context_hint = ""

    rows = ensure_imponibile_validation_fields(rows)
    issues = find_detail_imponibile_issues(rows)
    if not issues:
        return rows

    issues_have_recalc = any(str(issue.get("presenza_ricalcolo", "")).strip().lower() == "si" for issue in issues)
    if issues_have_recalc:
        grouped_rows = group_rows_by_period(rows)
        if all(
            should_prefer_detail_over_imponibile(
                grouped_rows.get(
                    (str(issue.get("data_inizio", "")).strip(), str(issue.get("data_fine", "")).strip()),
                    [],
                ),
                issue,
            )
            for issue in issues
        ):
            logging.warning(
                "[AUDIT] %s: documento con ricalcoli espliciti -> separo le righe di ricalcolo senza seconda rilettura GPT",
                pdf_path.name,
            )
            return apply_detail_sum_override(rows, issues)

        logging.warning(
            "[AUDIT] %s: documento con ricalcoli su %s periodo/i -> salto la seconda verifica imponibile/dettaglio e mantengo l'estrazione",
            pdf_path.name,
            len(issues),
        )
        return rows

    logging.warning(
        "[AUDIT] %s: dettaglio non coerente con imponibile su %s periodo/i -> seconda verifica GPT",
        pdf_path.name,
        len(issues),
    )

    reviewer = review_callback or review_gpt_with_pdf
    try:
        try:
            reviewed_rows = reviewer(pdf_path, model, issues, context_hint)
        except TypeError:
            reviewed_rows = reviewer(pdf_path, model, issues)
    except Exception as error:
        logging.warning("[AUDIT] %s: seconda verifica fallita: %s", pdf_path.name, error)
        reviewed_rows = None

    if reviewed_rows:
        reviewed_rows = ensure_imponibile_validation_fields(reviewed_rows)
        reviewed_rows = supplement_summary_macro_rows(pdf_path, reviewed_rows)
        reviewed_rows = normalize_detail_flags_from_rows(reviewed_rows)
        reviewed_issues = find_detail_imponibile_issues(reviewed_rows)
        if len(reviewed_issues) < len(issues) or mismatch_issue_score(reviewed_issues) < mismatch_issue_score(issues):
            rows = reviewed_rows
            issues = reviewed_issues

    if not issues:
        return rows

    if all(str(issue.get("presenza_ricalcolo", "")).strip().lower() != "si" for issue in issues):
        reconciled_rows = reconcile_standard_month_with_vat_summary(pdf_path, rows)
        reconciled_issues = find_detail_imponibile_issues(reconciled_rows)
        if len(reconciled_issues) < len(issues) or mismatch_issue_score(reconciled_issues) < mismatch_issue_score(issues):
            rows = reconciled_rows
            issues = reconciled_issues
        if not issues:
            return rows

        logging.warning(
            "[AUDIT] %s: nessun ricalcolo ma dettaglio e imponibile restano diversi -> tengo imponibile_mese e aggiungo warning",
            pdf_path.name,
        )
        note_text = (
            "Nonostante non ci siano ricalcoli, somma dettaglio e imponibile del documento sono diversi: "
            "usato imponibile_mese del documento"
        )
        adjusted_rows: list[dict] = []
        issue_keys = {
            (str(issue.get("data_inizio", "")).strip(), str(issue.get("data_fine", "")).strip())
            for issue in issues
        }
        for row in rows:
            adjusted = dict(row)
            key = (str(adjusted.get("data_inizio", "")).strip(), str(adjusted.get("data_fine", "")).strip())
            if key in issue_keys:
                note = str(adjusted.get("note", "")).strip()
                if note_text not in note:
                    adjusted["note"] = f"{note} | {note_text}".strip(" |")
            adjusted_rows.append(adjusted)
        return adjusted_rows

    if all(str(issue.get("presenza_ricalcolo", "")).strip().lower() == "si" for issue in issues):
        logging.warning(
            "[AUDIT] %s: documento con ricalcoli e dettaglio del mese non sufficientemente separato dopo seconda verifica -> uso imponibile_mese e marca manca_dettaglio=si",
            pdf_path.name,
        )
    else:
        logging.warning(
            "[AUDIT] %s: il dettaglio resta incoerente con l'imponibile dopo seconda verifica -> uso imponibile_mese e marca manca_dettaglio=si",
            pdf_path.name,
        )
    return mark_issues_as_missing_detail(rows, issues)


def normalize_decimal_for_export(value):
    if value is None or pd.isna(value):
        return ""

    text = str(value).strip()
    if not text:
        return ""

    if "." in text and "," in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")

    text = re.sub(r"[^0-9\.\-]", "", text)
    if text in ("", "-", ".", "-."):
        return ""

    try:
        normalized = format(Decimal(text).normalize(), "f")
    except InvalidOperation:
        return str(value)

    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    if normalized in ("-0", "-0.0"):
        normalized = "0"
    return normalized.replace(".", ",")


def normalize_export_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = df.copy()
    for column in DECIMAL_EXPORT_COLUMNS:
        if column in normalized_df.columns:
            normalized_df[column] = normalized_df[column].apply(normalize_decimal_for_export)
    return normalized_df


def prepare_export_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = normalize_export_dataframe(df)
    drop_columns = [column for column in HIDDEN_EXPORT_COLUMNS if column in normalized_df.columns]
    if drop_columns:
        normalized_df = normalized_df.drop(columns=drop_columns)
    ordered_columns = [column for column in EXPORT_PRIORITY_COLUMNS if column in normalized_df.columns]
    remaining_columns = [column for column in normalized_df.columns if column not in ordered_columns]
    return normalized_df[ordered_columns + remaining_columns]


def process_all_pdfs(
    input_dir: Path,
    pattern: str,
    out_csv: str,
    out_xlsx: str,
    resume: bool = True,
    retries: int = 3,
    sleep_s: float = 0.5,
) -> Optional[pd.DataFrame]:
    if not input_dir.is_dir():
        raise NotADirectoryError(f"Il percorso indicato non è una directory: {input_dir}")

    pdfs = find_pdf_files(input_dir, pattern)
    if not pdfs:
        logging.warning(f"Nessun PDF trovato in {input_dir} con pattern {pattern}")
        return None

    all_rows, processed = [], set()
    platform_alerts: list[str] = []
    manual_platform_alert = get_manual_platform_alert()
    if manual_platform_alert:
        platform_alerts.append(manual_platform_alert)
    if resume and Path(out_csv).exists():
        prev = pd.read_csv(out_csv, dtype=str)
        if "_source_file" in prev.columns:
            processed = set(prev["_source_file"].dropna().unique().tolist())
        all_rows.extend(prev.to_dict(orient="records"))
        logging.info(f"[RESUME] righe esistenti: {len(all_rows)} | file già fatti: {len(processed)}")

    for fp in pdfs:
        if fp.name in processed:
            logging.info(f"[SKIP] {fp.name}")
            continue

        logging.info(f"[PROCESS] {fp.name}")
        err = None
        for attempt in range(1, retries + 1):
            try:
                context_hint = build_recent_run_context(all_rows)
                try:
                    rows = extract_rows_from_pdf(fp, MODEL_PRIMARY, context_hint=context_hint)
                except Exception as error_primary:
                    logging.warning(f"  [WARN] {MODEL_PRIMARY} errore: {error_primary} -> provo {MODEL_FALLBACK}")
                    alert = detect_platform_alert_from_error(error_primary)
                    if alert and alert not in platform_alerts:
                        platform_alerts.append(alert)
                    rows = extract_rows_from_pdf(fp, MODEL_FALLBACK, context_hint=context_hint)
                rows = ensure_imponibile_validation_fields(rows)
                rows = supplement_summary_macro_rows(fp, rows)
                rows = normalize_detail_flags_from_rows(rows)
                rows = reconcile_standard_month_with_vat_summary(fp, rows)
                rows = recheck_rows_if_needed(fp, rows, MODEL_PRIMARY, context_hint=context_hint)
                rows = enrich_extracted_rows(rows)
                all_rows.extend(rows)
                err = None
                break
            except Exception as error:
                err = error
                alert = detect_platform_alert_from_error(error)
                if alert and alert not in platform_alerts:
                    platform_alerts.append(alert)
                wait = min(2 ** attempt, 15)
                logging.warning(f"  tentativo {attempt}/{retries} fallito: {error} -> retry in {wait}s")
                time.sleep(wait)

        if err:
            logging.error(f"[ERROR] {fp.name}: {err}")
            continue

        prepare_export_dataframe(pd.DataFrame(all_rows)).to_csv(out_csv, index=False, encoding="utf-8")
        logging.info(f"  +{len(rows)} righe (totale: {len(all_rows)})")
        if sleep_s > 0:
            time.sleep(sleep_s)

    if not all_rows:
        logging.info("[DONE] nessuna riga estratta.")
        return None

    df = prepare_export_dataframe(pd.DataFrame(all_rows))
    df.attrs["platform_alerts"] = platform_alerts
    df.to_excel(out_xlsx, index=False)
    logging.info(f"[DONE] Righe totali: {len(df)}")
    logging.info(f"  CSV : {out_csv}")
    logging.info(f"  XLSX: {out_xlsx}")

    return df


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    input_dir, searched_paths = resolve_input_dir(args.input_dir)

    if not input_dir.exists():
        searched = "\n".join(f"- {path}" for path in searched_paths)
        if args.input_dir:
            logging.error("Directory input non trovata. Percorsi controllati:\n%s", searched)
        else:
            logging.error(
                "Nessuna directory input trovata. Crea una delle seguenti cartelle o passane una esplicitamente:\n%s",
                searched,
            )
        return 1

    if not input_dir.is_dir():
        logging.error("Il percorso specificato non è una directory: %s", input_dir)
        return 1

    logging.info("Uso directory input: %s", input_dir)

    out_csv = str(root / "estrazione_tutti_mesi.csv")
    out_xlsx = str(root / "estrazione_tutti_mesi.xlsx")
    pattern = "*.pdf"

    df = process_all_pdfs(input_dir, pattern, out_csv, out_xlsx, RESUME, RETRIES, SLEEP_S)
    if df is not None:
        print(df.head(30))
    return 0


if __name__ == "__main__":
    sys.exit(main())
