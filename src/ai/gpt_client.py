import base64
import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
from openai import OpenAI

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.parser.bolletta_parser import parse_gpt_response

# Modello principale + fallback
MODEL_PRIMARY = os.environ.get("OPENAI_MODEL", "gpt-5")
MODEL_FALLBACK = "gpt-5"

_client: OpenAI | None = None

PROMPT = """
OBIETTIVO
Estrarre da una bolletta luce/gas:
A) METADATI (1 per documento)
B) RIGHE DI DETTAGLIO: voci economiche MENSILI (ammesse righe negative)

PERIODO DI RIFERIMENTO (REGOLA FONDAMENTALE)
- Considera SOLO il livello MENSILE.
- IGNORA completamente dettagli giornalieri, orari o per fasce.
- Se coesistono dati giornalieri e mensili, usa SOLO quelli mensili.
- Estrai solo: periodo mensile, consumi totali del mese, corrispettivi mensili.

PRINCIPI GENERALI
- Le RIGHE contengono solo VOCI ECONOMICHE reali del periodo: corrispettivi, trasporto, oneri, accise, addizionali, imposte di consumo, conguagli, storni, ricalcoli.
- Le righe negative sono AMMESSE e spesso rappresentano storni o ricalcoli: sono parte del dettaglio economico e NON vanno escluse solo perché negative.
- NON inserire nelle righe: Totali, Imponibile, IVA, Totale documento, Totale da pagare, Riepiloghi, Arrotondamenti, Subtotali.
- I valori di riepilogo vanno SOLO nei METADATI o in `imponibile_mese`.
- NON includere le more: non sono voci di consumo ma sanzioni/oneri extra.

ECCEZIONE IMPOSTE
- ACCISE, ADDIZIONALI e IMPOSTE DI CONSUMO DEVONO SEMPRE essere incluse come RIGHE.
- Se esistono come righe economiche singole o in dettaglio fiscale, includile normalmente.
- Se esistono SOLO come totale aggregato senza righe economiche che le compongono, includi una sola riga con `manca_dettaglio="sì"`.

DEFINIZIONE CORRETTA DI `manca_dettaglio`
- `manca_dettaglio` NON indica se la singola riga ha sottorighe figlie.
- `manca_dettaglio` indica se, nel documento, MANCA il dettaglio economico sufficiente per ricostruire il totale/imponibile del periodo.
- Imposta `manca_dettaglio="no"` quando il totale del periodo è ricostruibile dalle righe economiche presenti, anche se:
  - alcune righe sono negative
  - alcune righe sono storni
  - alcune righe sono ricalcoli o conguagli
  - alcune righe sono blocchi sintetici ma con importo economico chiaro
- Imposta `manca_dettaglio="sì"` SOLO quando il documento riporta un imponibile/totale del periodo ma NON riporta le righe economiche necessarie per ricostruirlo.
- Se nel documento sono presenti sia righe positive sia righe negative della stessa categoria (es. addebito + storno del calcolato in precedenza), questo conta come DETTAGLIO PRESENTE → `manca_dettaglio="no"`.
- Nelle bollette di conguaglio, ricalcolo o storno, la presenza di storni, acconti, recuperi o più imponibili nello stesso mese NON implica mancanza di dettaglio. Se il totale è ricostruibile dalle righe economiche, `manca_dettaglio="no"`.

SEZIONE A) METADATI (se presenti)
- fornitore_nome
- fornitore_piva
- numero_fattura
- data_fattura (gg/mm/aaaa)
- periodo_inizio
- periodo_fine
- cliente_nome
- cliente_piva_cf
- indirizzo_fornitura
- pod (luce)
- pdr (gas)
- tensione_alimentazione
- potenza_disponibile
- potenza_impegnata
- imponibile_mese
- totale_iva
- totale_documento
- totale_da_pagare
- aliquote_iva_applicate

SEZIONE B) RIGHE DI DETTAGLIO (1 riga = 1 voce mensile)
- NON creare righe per giorni, fasce o date puntuali.
- Ogni riga rappresenta una VOCE ECONOMICA DI PERIODO.
- Se una voce economica è presente in forma positiva e in forma negativa di storno, includi entrambe le righe.
- Mantieni il testo originale della bolletta in `dettaglio_voce`.

Campi per riga:
- nome_cliente
- pod / pdr
- data_inizio
- data_fine
- consumo_totale
- dettaglio_voce
- unita_misura
- quantita
- prezzo_aliquota
- importo
- imponibile_mese
- tensione_alimentazione
- potenza_disponibile
- potenza_impegnata
- data_indirizzo
- manca_dettaglio ("sì"/"no")
- note

REGOLE ANTI-DUPLICAZIONE
1) Escludi qualsiasi riga che sia un totale o riepilogo: Totale*, Imponibile*, IVA*, Riepilogo*, Arrotondamenti*, Totale documento*, Totale da pagare*.
2) Se una riga contiene la parola "Totale", trattala come riepilogo e NON inserirla come riga di dettaglio, salvo il caso in cui NON esistano altre righe economiche sottostanti per quella sezione e serva una sola riga aggregata.
3) Usa il BLOCCO DI DETTAGLIO economico; usa riepilogo/sintesi SOLO se il dettaglio economico non esiste davvero.
4) Categorie senza righe figlie vanno marcate con `manca_dettaglio="sì"` SOLO se il loro importo non è ricostruibile da altre righe economiche del documento.
5) Se il documento contiene abbastanza righe economiche da ricostruire il totale/imponibile del mese, allora tutte le righe economiche rilevanti devono avere `manca_dettaglio="no"`.

IMPUTAZIONE IMPONIBILE
- `imponibile_mese` rappresenta l'imponibile del periodo al netto IVA.
- NON confondere `imponibile_mese` con `totale_documento`, `totale_da_pagare` o `totale_iva`.
- Se il PDF riporta esplicitamente un imponibile IVA o una base imponibile del periodo, usa quello come `imponibile_mese`.
- Se il PDF riporta sia un totale spesa sia un imponibile fiscale, scegli come `imponibile_mese` la base imponibile del periodo al netto IVA.
- `imponibile_mese` deve essere coerente con il periodo mensile e non con il totale finale comprensivo di IVA.
- Se nello stesso documento esistono conguagli, storni o acconti, l'imponibile del periodo deve comunque essere quello riferito al periodo mensile estratto.

ISTRUZIONI IMPORTANTI SUI CONGUAGLI
- In presenza di conguagli, ricalcoli o storni:
  - includi le righe positive
  - includi le righe negative
  - non eliminare una riga solo perché compensa un'altra
  - non considerare gli storni come assenza di dettaglio
- Se una riga negativa descrive chiaramente una categoria economica (es. "Storno importo calcolato in precedenza ..."), allora è una riga economica valida.
- Se il documento consente di ricostruire il netto del mese tramite righe positive e negative, allora `manca_dettaglio="no"`.

NORMALIZZAZIONI
- Numeri: usa il punto come separatore decimale, nessun separatore delle migliaia, mantieni il segno.
- Date: gg/mm/aaaa.
- Testi: usa esattamente le diciture della bolletta.
- Mantieni il segno negativo sugli storni.

OUTPUT
- Restituisci SOLO JSON valido:
{
  "metadati": { ... },
  "rows": [ ... ]
}
- Nessun testo fuori dal JSON.
"""

JSON_SCHEMA = {
    "name": "righe_bolletta",
    "schema": {
        "type": "object",
        "properties": {
            "rows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "nome_cliente": {"type": "string"},
                        "pdr": {"type": "string"},
                        "pod": {"type": "string"},
                        "data_inizio": {"type": "string"},
                        "data_fine": {"type": "string"},
                        "consumo_totale": {"type": ["number", "string"]},
                        "dettaglio_voce": {"type": "string"},
                        "unita_misura": {"type": "string"},
                        "quantita": {"type": ["number", "string"]},
                        "prezzo_aliquota": {"type": ["number", "string"]},
                        "importo": {"type": ["number", "string"]},
                        "imponibile_mese": {"type": ["number", "string"]},
                        "note": {"type": "string"},
                        "tensione_alimentazione": {"type": "string"},
                        "manca_dettaglio": {"type": "string"},
                        "potenza_disponibile": {"type": "string"},
                        "potenza_impegnata": {"type": "string"},
                        "data_indirizzo": {"type": "string"}
                    },
                    "required": [
                        "nome_cliente",
                        "pdr",
                        "data_inizio",
                        "data_fine",
                        "consumo_totale",
                        "dettaglio_voce",
                        "importo",
                        "imponibile_mese",
                        "manca_dettaglio"
                    ]
                }
            }
        },
        "required": ["rows"],
        "additionalProperties": False
    },
    "strict": True
}


def ensure_openai_api_key() -> None:
    """Load OPENAI_API_KEY lazily to avoid side effects at import time."""
    if os.environ.get("OPENAI_API_KEY"):
        return

    from src.utils.file_utils import load_api_key

    api_key = load_api_key()
    if api_key:
        os.environ["OPENAI_API_KEY"] = api_key
        return

    from getpass import getpass

    os.environ["OPENAI_API_KEY"] = getpass("YourApi")


def get_openai_client() -> OpenAI:
    """Create the OpenAI client only when it is actually needed."""
    global _client
    if _client is None:
        ensure_openai_api_key()
        _client = OpenAI()
    return _client


def build_pdf_input_content(pdf_path: Path) -> dict[str, str]:
    """Build an input_file payload with a normalized .pdf filename."""
    with open(pdf_path, "rb") as f:
        base64_string = base64.b64encode(f.read()).decode("utf-8")

    return {
        "type": "input_file",
        "filename": f"{pdf_path.stem}.pdf",
        "file_data": f"data:application/pdf;base64,{base64_string}",
    }


def call_gpt_with_pdf(pdf_path: Path, model: str) -> List[Dict[str, Any]]:
    """Call GPT with the PDF and return parsed rows."""
    if not pdf_path.exists():
        raise FileNotFoundError(f"File non trovato: {pdf_path}")

    client = get_openai_client()
    input_file = build_pdf_input_content(pdf_path)

    # Tentativo A: Structured Outputs con response_format
    try:
        rsp = client.responses.create(
            model=model,
            instructions="Analizza il PDF e restituisci SOLO JSON aderente allo schema.",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": PROMPT},
                    input_file,
                ]
            }],
            response_format={"type": "json_schema", "json_schema": JSON_SCHEMA},
            max_output_tokens=200_000
        )
        raw = getattr(rsp, "output_text", None)
        if not raw:
            parts = []
            for out in getattr(rsp, "output", []) or []:
                for c in getattr(out, "content", []) or []:
                    if getattr(c, "type", "") == "output_text":
                        parts.append(c.text)
            raw = "".join(parts)
    except TypeError:
        # Tentativo B: SDK senza response_format -> vincolo via prompt
        rsp = client.responses.create(
            model=model,
            input=[{
                "role": "user",
                "content": [
                    input_file,
                    {"type": "input_text", "text":
                        f"{PROMPT}\n\nRESTITUISCI SOLO JSON valido con radice {{\"rows\": [...]}} "
                        f"aderente ESATTAMENTE a questo schema (nessun testo extra):\n{json.dumps(JSON_SCHEMA)}"
                    },
                ]
            }],
            max_output_tokens=200_000
        )
        raw = getattr(rsp, "output_text", "") or ""
        if not raw:
            parts = []
            for out in getattr(rsp, "output", []) or []:
                for c in getattr(out, "content", []) or []:
                    if getattr(c, "type", "") == "output_text":
                        parts.append(c.text)
            raw = "".join(parts)
        s, e = raw.find("{"), raw.rfind("}")
        if s >= 0 and e > s:
            raw = raw[s:e+1]
 
    if not raw:
        raise RuntimeError(f"Risposta vuota dal modello su: {pdf_path.name}")

    rows = parse_gpt_response(raw, pdf_path.name)
    return rows
