import json
import logging
from typing import List, Dict, Any

ROW_DEFAULTS = {
    "consumo_dettaglio_riga": "",
    "manca_dettaglio_consumo": "",
}


def parse_gpt_response(raw_response: str, source_file: str) -> List[Dict[str, Any]]:
    """Parse the raw JSON response from GPT and return the rows with source file."""
    try:
        data = json.loads(raw_response)
        rows = data.get("rows", [])
        for r in rows:
            for key, default_value in ROW_DEFAULTS.items():
                r.setdefault(key, default_value)
            r["_source_file"] = source_file
        return rows
    except json.JSONDecodeError as e:
        logging.error(f"Errore nel parsing JSON: {e}")
        raise
