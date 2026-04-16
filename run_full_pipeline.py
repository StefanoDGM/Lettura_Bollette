"""
Full Pipeline per Estrazione Bollette Energetiche

Questo script è il punto di ingresso unico per elaborare bollette PDF e generare:
1. Dati grezzi estratti (CSV + XLSX)
2. Riepilogo mensile aggregato (XLSX)

Flusso:
- Trova tutti i PDF nella directory input
- Per ogni PDF: limita pagine → invia a GPT → estrai JSON → salva righe
- Aggrega righe per mese: calcola totali importi e consumo moda
- Salva output finali

Uso: python run_full_pipeline.py <input_dir>
Es: python run_full_pipeline.py "tests/data"

Codice autonomo: include tutta la logica senza dipendenze esterne.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Assicura che il package "src" sia importabile (da root)
# Inseriamo la root del progetto (parent di src) nel path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.pipeline.process_bolletta import process_all_pdfs
from src.pipeline.aggregate_bills import aggregate_bolletta_data

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Parametri
RESUME = False
RETRIES = 3
SLEEP_S = 0.5


def get_pipeline_output_paths(output_dir: Optional[Path] = None) -> dict[str, Path]:
    """Resolve the three pipeline report paths."""
    base_dir = Path(output_dir) if output_dir is not None else Path.cwd()
    base_dir.mkdir(parents=True, exist_ok=True)
    return {
        "csv": base_dir / "estrazione_tutti_mesi.csv",
        "xlsx": base_dir / "estrazione_tutti_mesi.xlsx",
        "aggregated": base_dir / "bollette_raggruppate.xlsx",
    }


def extract_warning_report(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """Return only months that contain final warnings for the interface/report layer."""
    if aggregated_df is not None:
        attr_report = aggregated_df.attrs.get("warning_report")
        if isinstance(attr_report, pd.DataFrame):
            return attr_report.copy()

    if aggregated_df is None or aggregated_df.empty or "warning_count" not in aggregated_df.columns:
        return pd.DataFrame()

    warning_df = aggregated_df.loc[
        aggregated_df["warning_count"].fillna(0).astype(int) > 0,
        [column for column in [
            "anno",
            "mese",
            "warning_count",
            "warning_mese",
            "source_file_elenco",
            "metodi_ripartizione_ricalcolo",
            "consumo_logica_usata",
            "importo_logica_usata",
        ] if column in aggregated_df.columns],
    ].copy()
    return warning_df.reset_index(drop=True)


def full_pipeline(input_dir: Path, output_dir: Optional[Path] = None) -> Optional[dict]:
    """Esegue estrazione + aggregazione utilizzando gli script componenti."""
    output_paths = get_pipeline_output_paths(output_dir)
    pattern = "*.pdf"

    extracted_df = process_all_pdfs(
        input_dir,
        pattern,
        str(output_paths["csv"]),
        str(output_paths["xlsx"]),
        RESUME,
        RETRIES,
        SLEEP_S,
    )
    if extracted_df is None:
        return None

    aggregated_df = aggregate_bolletta_data(output_paths["csv"], output_paths["aggregated"])
    return {
        "extracted": extracted_df,
        "aggregated": aggregated_df,
        "warnings": extract_warning_report(aggregated_df),
        "files": output_paths,
        "platform_alerts": extracted_df.attrs.get("platform_alerts", []) if extracted_df is not None else [],
    }


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python run_full_pipeline.py <input_dir>")
        sys.exit(1)

    input_dir = Path(sys.argv[1])
    if not input_dir.exists():
        logging.error(f"Directory {input_dir} does not exist")
        sys.exit(1)

    dfs = full_pipeline(input_dir)
    if dfs is not None:
        print(dfs["extracted"].head(30))
