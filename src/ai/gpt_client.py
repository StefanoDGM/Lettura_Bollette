import base64
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from openai import OpenAI

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.parser.bolletta_parser import parse_gpt_response

# Modello principale + fallback
MODEL_PRIMARY = os.environ.get("OPENAI_MODEL", "gpt-5")
MODEL_FALLBACK = "gpt-5"

_client: OpenAI | None = None

PROMPT = """
OBIETTIVO
Estrarre da una bolletta luce/gas:
A) METADATI (1 per documento)
B) RIGHE DI DETTAGLIO MENSILI

PERIODO DI RIFERIMENTO
- Considera SOLO il livello mensile.
- Ignora dettagli giornalieri, orari o per fasce.
- Se coesistono dati giornalieri e mensili, usa SOLO quelli mensili.
- Ogni riga deve riferirsi a un periodo mensile coerente.

PRINCIPI GENERALI
- Le righe contengono solo voci economiche reali del periodo: corrispettivi, trasporto, oneri, imposte, conguagli, ricalcoli, storni.
- Le righe negative sono ammesse e vanno mantenute.
- NON inserire righe per totali, imponibile, IVA, totale documento, totale da pagare, riepiloghi, arrotondamenti, subtotali o preventivi dei mesi successivi.
- I valori di riepilogo vanno nei metadati o in `imponibile_mese`.
- NON includere le more: non sono voci di consumo ma sanzioni/oneri extra.
- NON includere preventivi dei mesi successivi: non sono ancora voci economiche ufficiali del periodo e dovrebbero comparire nelle bollette successive come voci reali.

ECCEZIONE IMPOSTE
- Accise, addizionali e imposte di consumo devono sempre essere incluse come righe.
- Se esistono come righe economiche singole o in dettaglio fiscale, includile normalmente.
- Se esistono solo come totale aggregato senza righe economiche che le compongono, includi una sola riga con `manca_dettaglio="si"`.

DEFINIZIONE CORRETTA DI `manca_dettaglio`
- `manca_dettaglio` e' un flag documentale per il dettaglio economico.
- Vale "no" quando il documento contiene abbastanza righe economiche da ricostruire il netto/imponibile del periodo.
- Vale "si" solo quando esiste un totale/imponibile del periodo ma mancano le righe economiche necessarie a ricostruirlo.
- Se esistono righe positive e negative di storno/ricalcolo che consentono di ricostruire il netto, allora `manca_dettaglio="no"`.
- Nelle bollette di conguaglio, ricalcolo o storno, la presenza di storni, acconti, recuperi o piu imponibili nello stesso mese NON implica mancanza di dettaglio. Se il totale e' ricostruibile dalle righe economiche, `manca_dettaglio="no"`.

DEFINIZIONE CORRETTA DI `manca_dettaglio_consumo`
- `manca_dettaglio_consumo` e' un flag documentale per il dettaglio dei consumi.
- Vale "no" quando il documento contiene abbastanza righe di quantita con segno per ricostruire il consumo del periodo.
- Vale "si" quando esiste un `consumo_totale` del periodo ma NON ci sono righe di quantita sufficienti per ricostruirlo.
- Il flag non si decide guardando la singola riga: valuta la ricostruibilita del consumo nell'intero documento per quel periodo.
- Ripeti lo stesso valore di `manca_dettaglio_consumo` su tutte le righe dello stesso documento/periodo.

CONSUMI: DIFFERENZA TRA TOTALE E DETTAGLIO
- `consumo_totale` = consumo totale dichiarato per il periodo nel documento.
- `consumo_dettaglio_riga` = quantita con segno SOLO quando la riga rappresenta davvero consumo del periodo o storno/ricalcolo di consumo.
- Usa `consumo_dettaglio_riga` per righe come energia/consumo/prelievo/gas/storno consumo/ricalcolo quantita.
- NON usare `consumo_dettaglio_riga` su righe economiche che mostrano kWh/Smc/mc solo come base tariffaria ma non sono una riga di consumo autonoma: trasporto, oneri, perdite, dispacciamento, quota fissa, quota potenza, imposte.
- Se una riga di consumo e' uno storno o ricalcolo, mantieni il segno negativo/positivo in `consumo_dettaglio_riga`.
- Se il documento non consente di ricostruire il consumo tramite righe di quantita, lascia `consumo_dettaglio_riga` vuoto e imposta `manca_dettaglio_consumo="si"`.

SEZIONE A) METADATI
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

SEZIONE B) RIGHE DI DETTAGLIO
- Una riga = una voce economica mensile.
- Mantieni il testo originale della bolletta in `dettaglio_voce`.
- Se una voce economica e' presente in forma positiva e in forma negativa, includi entrambe.

CAMPI PER RIGA
- nome_cliente
- pod / pdr
- data_inizio
- data_fine
- consumo_totale
- consumo_dettaglio_riga
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
- manca_dettaglio ("si"/"no")
- manca_dettaglio_consumo ("si"/"no")
- note

REGOLE ANTI-DUPLICAZIONE
1) Escludi qualsiasi riga che sia un totale o riepilogo: Totale*, Imponibile*, IVA*, Riepilogo*, Arrotondamenti*, Totale documento*, Totale da pagare*.
2) Se una riga contiene la parola "Totale", trattala come riepilogo e NON inserirla come riga di dettaglio, salvo il caso in cui NON esistano altre righe economiche sottostanti per quella sezione e serva una sola riga aggregata.
3) Usa il blocco di dettaglio economico; usa riepilogo/sintesi solo se il dettaglio economico non esiste davvero.
4) Se il documento contiene abbastanza righe economiche per ricostruire il netto, `manca_dettaglio="no"`.
5) Se il documento contiene abbastanza righe di quantita per ricostruire il consumo, `manca_dettaglio_consumo="no"`.

IMPUTAZIONE IMPONIBILE
- `imponibile_mese` rappresenta l'imponibile del periodo al netto IVA.
- NON confondere `imponibile_mese` con `totale_documento`, `totale_da_pagare` o `totale_iva`.
- Se il PDF riporta esplicitamente un imponibile IVA o una base imponibile del periodo, usa quello come `imponibile_mese`.
- Se il PDF riporta sia un totale spesa sia un imponibile fiscale, scegli come `imponibile_mese` la base imponibile del periodo al netto IVA.
- `imponibile_mese` deve essere coerente con il periodo mensile e non con il totale finale comprensivo di IVA.
- Se nello stesso documento esistono conguagli, storni o acconti, l'imponibile del periodo deve comunque essere quello riferito al periodo mensile estratto.

ISTRUZIONI IMPORTANTI SU CONGUAGLI E STORNI
- In presenza di conguagli, ricalcoli o storni:
  - includi le righe positive
  - includi le righe negative
  - non eliminare una riga solo perche compensa un'altra
  - non considerare gli storni come assenza di dettaglio
- Se una riga negativa descrive chiaramente una categoria economica (es. "Storno importo calcolato in precedenza ..."), allora e' una riga economica valida.
- Se il documento consente di ricostruire il netto del mese tramite righe positive e negative, allora `manca_dettaglio="no"`.

NORMALIZZAZIONI
- Numeri: usa il punto come separatore decimale, senza separatore migliaia, mantenendo il segno.
- Date: gg/mm/aaaa.
- Testi: usa le diciture della bolletta.
- Se un campo non e' applicabile alla riga, restituisci stringa vuota.

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
                        "consumo_dettaglio_riga": {"type": ["number", "string"]},
                        "dettaglio_voce": {"type": "string"},
                        "unita_misura": {"type": "string"},
                        "quantita": {"type": ["number", "string"]},
                        "prezzo_aliquota": {"type": ["number", "string"]},
                        "importo": {"type": ["number", "string"]},
                        "imponibile_mese": {"type": ["number", "string"]},
                        "note": {"type": "string"},
                        "tensione_alimentazione": {"type": "string"},
                        "manca_dettaglio": {"type": "string"},
                        "manca_dettaglio_consumo": {"type": "string"},
                        "potenza_disponibile": {"type": "string"},
                        "potenza_impegnata": {"type": "string"},
                        "data_indirizzo": {"type": "string"},
                    },
                    "required": [
                        "nome_cliente",
                        "pdr",
                        "data_inizio",
                        "data_fine",
                        "consumo_totale",
                        "consumo_dettaglio_riga",
                        "dettaglio_voce",
                        "importo",
                        "imponibile_mese",
                        "manca_dettaglio",
                        "manca_dettaglio_consumo",
                    ],
                },
            }
        },
        "required": ["rows"],
        "additionalProperties": False,
    },
    "strict": True,
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
    with open(pdf_path, "rb") as file_obj:
        base64_string = base64.b64encode(file_obj.read()).decode("utf-8")

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

    try:
        rsp = client.responses.create(
            model=model,
            instructions="Analizza il PDF e restituisci SOLO JSON aderente allo schema.",
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": PROMPT},
                    input_file,
                ],
            }],
            response_format={"type": "json_schema", "json_schema": JSON_SCHEMA},
            max_output_tokens=200_000,
        )
        raw = getattr(rsp, "output_text", None)
        if not raw:
            parts = []
            for out in getattr(rsp, "output", []) or []:
                for content in getattr(out, "content", []) or []:
                    if getattr(content, "type", "") == "output_text":
                        parts.append(content.text)
            raw = "".join(parts)
    except TypeError:
        rsp = client.responses.create(
            model=model,
            input=[{
                "role": "user",
                "content": [
                    input_file,
                    {
                        "type": "input_text",
                        "text": (
                            f"{PROMPT}\n\nRESTITUISCI SOLO JSON valido con radice {{\"rows\": [...]}} "
                            f"aderente ESATTAMENTE a questo schema (nessun testo extra):\n{json.dumps(JSON_SCHEMA)}"
                        ),
                    },
                ],
            }],
            max_output_tokens=200_000,
        )
        raw = getattr(rsp, "output_text", "") or ""
        if not raw:
            parts = []
            for out in getattr(rsp, "output", []) or []:
                for content in getattr(out, "content", []) or []:
                    if getattr(content, "type", "") == "output_text":
                        parts.append(content.text)
            raw = "".join(parts)
        start, end = raw.find("{"), raw.rfind("}")
        if start >= 0 and end > start:
            raw = raw[start:end + 1]

    if not raw:
        raise RuntimeError(f"Risposta vuota dal modello su: {pdf_path.name}")

    return parse_gpt_response(raw, pdf_path.name)
