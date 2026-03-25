"""
Script Separato per Estrazione Dati da PDF Bollette

Questo script fa SOLO l'estrazione dei dati dai PDF.
Genera: estrazione_tutti_mesi.csv + .xlsx

Uso: python src/pipeline/process_bolletta.py [input_dir]
Es: python src/pipeline/process_bolletta.py data

Se non specificato, usa "data" nella root progetto e, se assente, ripiega su "tests/data".

Codice autonomo: include tutta la logica di estrazione senza dipendenze esterne.
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional
import pandas as pd

# Aggiungi root del progetto al path per import moduli (per poter importare il package `src`)
# `parents[2]` = project root (genitore di `src`)
root = Path(__file__).resolve().parents[2]
# Se si esegue lo script da dentro src/pipeline, la cwd cambia e i moduli non si trovano.
# Aggiungiamo sia la root che la cartella src al sys.path.
sys.path.insert(0, str(root))
sys.path.insert(0, str(root / "src"))

from src.extractor.pdf_extractor import limit_pdf_pages
from src.ai.gpt_client import call_gpt_with_pdf, MODEL_PRIMARY, MODEL_FALLBACK

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Parametri
RESUME = False
RETRIES = 3
SLEEP_S = 0.5
DEFAULT_INPUT_DIR_CANDIDATES = (
    root / "data",
    root / "tests" / "data",
)


def build_arg_parser() -> argparse.ArgumentParser:
    """Create CLI parser for the standalone extraction script."""
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
    """Resolve input dir from CLI, supporting both cwd-relative and project-relative paths."""
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
    """Find PDF files, accepting both .pdf and .PDF extensions."""
    if pattern.lower() == "*.pdf":
        return sorted(
            path for path in input_dir.iterdir()
            if path.is_file() and path.suffix.lower() == ".pdf"
        )
    return sorted(input_dir.glob(pattern))


def extract_rows_from_pdf(pdf_path: Path, model: str) -> list[dict]:
    """Extract rows from a single PDF."""
    pdf_to_use = limit_pdf_pages(pdf_path)
    return call_gpt_with_pdf(pdf_to_use, model)


def process_all_pdfs(input_dir: Path,
                     pattern: str,
                     out_csv: str,
                     out_xlsx: str,
                     resume: bool = True,
                     retries: int = 3,
                     sleep_s: float = 0.5) -> Optional[pd.DataFrame]:
    """Elabora tutti i PDF nella directory input e salva dati estratti."""
    if not input_dir.is_dir():
        raise NotADirectoryError(f"Il percorso indicato non è una directory: {input_dir}")

    pdfs = find_pdf_files(input_dir, pattern)
    if not pdfs:
        logging.warning(f"Nessun PDF trovato in {input_dir} con pattern {pattern}")
        return None

    all_rows, processed = [], set()
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
                try:
                    rows = extract_rows_from_pdf(fp, MODEL_PRIMARY)
                except Exception as e1:
                    logging.warning(f"  [WARN] {MODEL_PRIMARY} errore: {e1} → provo {MODEL_FALLBACK}")
                    rows = extract_rows_from_pdf(fp, MODEL_FALLBACK)
                all_rows.extend(rows)
                err = None
                break
            except Exception as e:
                err = e
                wait = min(2 ** attempt, 15)
                logging.warning(f"  tentativo {attempt}/{retries} fallito: {e} → retry in {wait}s")
                time.sleep(wait)

        if err:
            logging.error(f"[ERROR] {fp.name}: {err}")
            continue

        pd.DataFrame(all_rows).to_csv(out_csv, index=False, encoding="utf-8")
        logging.info(f"  +{len(rows)} righe (totale: {len(all_rows)})")
        if sleep_s > 0:
            time.sleep(sleep_s)

    if not all_rows:
        logging.info("[DONE] nessuna riga estratta.")
        return None

    df = pd.DataFrame(all_rows)
    df.to_excel(out_xlsx, index=False)
    logging.info(f"[DONE] Righe totali: {len(df)}")
    logging.info(f"  CSV : {out_csv}")
    logging.info(f"  XLSX: {out_xlsx}")

    return df


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entrypoint for standalone extraction."""
    args = build_arg_parser().parse_args(argv)
    input_dir, searched_paths = resolve_input_dir(args.input_dir)

    if not input_dir.exists():
        if args.input_dir:
            searched = "\n".join(f"- {path}" for path in searched_paths)
            logging.error("Directory input non trovata. Percorsi controllati:\n%s", searched)
        else:
            searched = "\n".join(f"- {path}" for path in searched_paths)
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
        print(df.head(30))  # For notebook-like output
    return 0


if __name__ == "__main__":
    sys.exit(main())
