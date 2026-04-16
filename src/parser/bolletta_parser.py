import json
import logging
from typing import Any, Dict, List

ROW_DEFAULTS = {
    "consumo_dettaglio_riga": "",
    "manca_dettaglio_consumo": "",
    "tipo_componente": "",
    "riferimento_ricalcolo_da": "",
    "riferimento_ricalcolo_a": "",
    "presenza_ricalcolo": "",
    "ricalcolo_aggregato_multi_mese": "",
}


def parse_gpt_response(raw_response: str, source_file: str) -> List[Dict[str, Any]]:
    """Parse the raw JSON response from GPT and return the rows with source file."""
    try:
        data = json.loads(raw_response)
        rows = data.get("rows", [])
        for row in rows:
            for key, default_value in ROW_DEFAULTS.items():
                row.setdefault(key, default_value)
            row["_source_file"] = source_file
        return rows
    except json.JSONDecodeError as error:
        logging.error(f"Errore nel parsing JSON: {error}")
        raise
