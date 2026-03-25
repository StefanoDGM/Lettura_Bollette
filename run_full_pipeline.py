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


def full_pipeline(input_dir: Path) -> Optional[dict]:
    """Esegue estrazione + aggregazione utilizzando gli script componenti."""
    out_csv = "estrazione_tutti_mesi.csv"
    out_xlsx = "estrazione_tutti_mesi.xlsx"
    pattern = "*.pdf"

    extracted_df = process_all_pdfs(input_dir, pattern, out_csv, out_xlsx, RESUME, RETRIES, SLEEP_S)
    if extracted_df is None:
        return None

    aggregated_df = aggregate_bolletta_data(out_csv, "bollette_raggruppate.xlsx")
    return {"extracted": extracted_df, "aggregated": aggregated_df}


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
