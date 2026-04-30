import base64
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import OpenAI

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.parser.bolletta_parser import parse_gpt_response

# Modello principale + fallback
MODEL_PRIMARY = os.environ.get("OPENAI_MODEL", "gpt-5.1")
MODEL_FALLBACK = "gpt-5.1"
ROOT = Path(__file__).resolve().parents[2]

_client: OpenAI | None = None

PROMPT = """
OBIETTIVO
Estrarre da una bolletta luce/gas SOLO righe economiche mensili e i campi necessari al post-processing finale.

PRINCIPIO
Analizza le bollette e NON calcolare risultati finali su piu bollette.
Estrai solo dati corretti e coerenti del singolo documento.

PERIODO
- Usa SOLO il livello mensile.
- Ignora dettagli giornalieri, orari o per fasce.
- Se coesistono dati giornalieri e mensili, usa i mensili.
- Ogni riga deve avere un periodo mensile coerente.

RIGHE DA INCLUDERE
Solo voci economiche reali del periodo, positive o negative:
- vendita, trasporto/distribuzione, oneri, imposte/accise/addizionali
- conguagli, storni, ricalcoli

RIGHE DA ESCLUDERE
tutto ciò che non è una voce economica mensile reale del periodo, come ad esempio:
NON creare righe per:
- Totale*
- Imponibile*
- IVA*
- Totale documento
- Totale da pagare
- Riepilogo*
- Arrotondamenti*
- Subtotali
- Preventivi di mesi successivi
- More / sanzioni / solleciti

  ECCEZIONE IMPOSTE
  Accise, addizionali e imposte di consumo vanno incluse come righe.
  Se esistono solo come totale aggregato, includi una sola riga e usa `manca_dettaglio="si"`.
  - NON includere automaticamente `Altre partite` / `Oneri diversi`.
  - Includi una voce in `Altre partite` solo se rappresenta chiaramente un costo energetico del mese, con periodo, quantita, prezzo o componenti energia/rete/oneri/imposte.
  - Escludi invece le `Altre partite` che sono solo partite finanziarie/contabili: `Anticipo fornitura E.E.`, compensazioni/restituzioni di anticipo, depositi/cauzioni, interessi, more, solleciti, imposta di bollo.
  - Un blocco `Acconto [mese]` o `Conguaglio [mese]` con righe energetiche dettagliate del mese va invece incluso nel dettaglio mensile.

MANCANZA DI DETTAGLIO ECONOMICO
- `manca_dettaglio` e' un flag documentale.
- `no` se le righe economiche bastano a ricostruire netto/imponibile del periodo.
- `si` solo se esiste un totale/imponibile ma mancano le righe economiche per ricostruirlo.
- Se righe positive e negative di storno/ricalcolo bastano a ricostruire il netto, usa `no`.
- Ripeti lo stesso valore su tutte le righe dello stesso documento/periodo.

CONSUMI
- `consumo_totale` = consumo totale dichiarato per il PERIODO DELLA RIGA / DEL MESE ESTRATTO, non il totale complessivo del documento se il documento copre piu mesi.
- `consumo_dettaglio_riga` = quantita con segno SOLO se la riga rappresenta davvero consumo del periodo o storno/ricalcolo di consumo.
- Casi validi: una singola riga autonoma di consumo del periodo, oppure un ricalcolo/storno con segni.
- Le normali righe importi che ripetono kWh/Smc/mc solo come base tariffaria NON sono dettaglio consumo.
- Se la stessa quantita e' solo base tariffaria su righe come CCR, prezzo mercato, trasporto, oneri, perdite, dispacciamento, quote fisse/potenza, imposte ecc., lascia `consumo_dettaglio_riga=""` e usa `manca_dettaglio_consumo="si"`.
- Se serve una riga separata per rappresentare correttamente il consumo, creala con `importo=""`.
- Se in bollette successive compare un valore vecchio e uno nuovo per lo stesso mese, NON sommarli come dettaglio economico: estrai solo eventuali righe di consumo/storno e il `consumo_totale`.
- `manca_dettaglio_consumo="no"` solo se il documento consente davvero di ricostruire il consumo tramite righe di quantita.
- Se esiste `consumo_totale` ma manca una vera riga autonoma di consumo oppure una coppia di ricalcolo/storno con segni sufficiente a ricostruirlo, usa `si`.
- `manca_dettaglio_consumo` e' un flag documentale: ripetilo su tutte le righe dello stesso documento/periodo.
- Se il documento ha un periodo generale multi-mese ma nelle righe del dettaglio o nelle tabelle consumi compaiono quantita separate per il singolo mese, usa per `consumo_totale` il valore del singolo mese e NON il totale aggregato del documento.
- Esempio: se il documento dice totale fatturato luglio+agosto = 74.569 ma le righe del mese agosto usano 15.037, allora per agosto `consumo_totale=15037`.
- Questa logica vale sia per GAS sia per LUCE.
- Nelle bollette elettriche usa lo stesso criterio: privilegia il valore mensile del mese, anche se il documento contiene fasce F1/F2/F3, energia, perdite, potenza o tabelle riassuntive.
- Le fasce F1/F2/F3 o le componenti di potenza possono aiutare a capire il consumo del mese, ma non implicano da sole che l'importo del mese sia ricostruibile con certezza.

FLAG DI RIGA OBBLIGATORI
- `presenza_ricalcolo` = "si" SOLO sulla riga che e' essa stessa uno storno/conguaglio/ricalcolo/acconto precedente o una rettifica riferita a un altro periodo.
- Se nello stesso documento ci sono 200 righe normali del mese corrente e 1 riga riferita a un mese precedente, metti `presenza_ricalcolo="si"` SOLO sulla riga del mese precedente; tutte le righe normali del mese corrente restano `no`.
- `ricalcolo_aggregato_multi_mese` = "si" SOLO sulla singola riga che rappresenta davvero un ricalcolo/storno riferito a piu mesi insieme o a un intervallo multi-mese non allocabile direttamente a un solo mese.
- Se il riferimento del ricalcolo e' un solo mese (es. `01/05/2024` -> `31/05/2024`), allora `presenza_ricalcolo="si"` ma `ricalcolo_aggregato_multi_mese="no"`.
- `tipo_ricalcolo` deve valere SOLO su righe con `presenza_ricalcolo="si"` e puo essere:
  - `importo` = rettifica economica/tariffaria che non modifica il consumo del mese
  - `consumo` = rettifica del consumo/quantita del mese senza importo economico affidabile
  - `importo_e_consumo` = rettifica che coinvolge sia importi sia consumo/quantita
- Se il PDF mostra che l`imponibile/totale fiscale del mese include anche partite finanziarie o contabili escluse dal dettaglio energetico del mese
  (per esempio `Altre partite`, `Anticipo fornitura E.E.`, compensazioni di anticipo, cauzioni, bollo),
  marca le righe energetiche del mese con `presenza_ricalcolo="si"` e `tipo_ricalcolo="importo"` cosi il mese venga trattato come rettificato.
- Usa questa marcatura SOLO quando il PDF mostra chiaramente che la differenza nasce da partite finanziarie/contabili estranee al costo energetico del mese,
  NON per qualsiasi semplice mismatch tra dettaglio e imponibile.
- Se il PDF contiene solo diciture generiche come `(conguaglio)` nel titolo del periodo, `TOTALE BOLLETTA (con riserva di conguaglio)` o formule standard analoghe, NON basta da solo per mettere `presenza_ricalcolo="si"`.
- In assenza di una vera riga di rettifica, lascia `presenza_ricalcolo="no"`, `ricalcolo_aggregato_multi_mese="no"` e `tipo_ricalcolo=""`.
- NON trattare questi flag come documentali e NON ripeterli su tutte le righe dello stesso documento.

RICALCOLI AGGREGATI
- Se una riga appartiene a un blocco come "Ricalcoli dal ... al ..." o a un ricalcolo aggregato riferito a piu mesi precedenti, valorizza i campi dedicati.
- NON distribuire il ricalcolo sui mesi originari.
- Privilegia righe con periodo/data esplicita.
- Se una riga non ha data specifica, includila solo se utile a capire il costo maturato; se il blocco resta troppo aggregato, mantienilo come ricalcolo aggregato e segnala dettaglio insufficiente.
- Se compaiono righe riferite a mesi diversi, mantienile separate con il loro periodo corretto.
- Se una riga e' un ricalcolo di altri mesi, NON farla contribuire al dettaglio del mese corrente: assegnale il proprio periodo o mantienila come ricalcolo separato.

CLASSIFICAZIONE COMPONENTE
Usa SOLO uno di questi valori per `tipo_componente`:
- `fissa` = quote/canoni indipendenti dal consumo
- `variabile` = energia, gas, consumo, prelievo, voci dipendenti da kWh/Smc/mc
- `imposte` = accise, addizionali, imposte di consumo
- `trasporto` = trasporto, distribuzione, dispacciamento chiaramente classificabili
- `oneri` = oneri di sistema o analoghi
- `ricalcolo_aggregato` = ricalcolo multi-mese presente ma non classificabile con sufficiente certezza
- `altro` = ultima scelta
- Esempi pratici gas A2A: `Quota Fissa` o unita come `€/mese/IG` => `fissa`; `Quota Proporzionale rispetto ai consumi` o `€/Smc` sulla materia gas => `variabile`; `CRVBL`, `CRVI`, `CVu`, trasporto/contatore => `trasporto`; accise/addizionali => `imposte`.

CAMPI RICALCOLO
- `riferimento_ricalcolo_da` e `riferimento_ricalcolo_a` = intervallo del ricalcolo se esplicitato
- Se la riga NON appartiene a un ricalcolo o non ha intervallo esplicito, lascia vuoti questi campi.

TABELLE DI DETTAGLIO MENSILI
- Se il documento contiene una tabella esplicita come "Elementi di dettaglio dal ... al ...", considera valide come righe tutte le voci economiche sottostanti di vendita, trasporto/distribuzione e imposte.
- In questo caso NON restituire `rows: []` solo perche nella stessa pagina esistono anche riepiloghi o totali.
- Escludi solo le righe di totale, IVA, subtotale, totale documento o totale da pagare.
- Se nella stessa tabella la quantita del mese e' solo ripetuta sulle righe economiche come base di calcolo, NON copiare quella quantita in `consumo_dettaglio_riga` su tutte le righe.

IMPUTAZIONE IMPONIBILE
- `imponibile_mese` = imponibile del periodo al netto IVA.
- NON confonderlo con `totale_documento`, `totale_da_pagare` o `totale_iva`.
- Se esiste una base imponibile del periodo esplicita, usa quella; se esistono sia totale spesa sia imponibile fiscale, usa la base imponibile al netto IVA.
- Nei casi standard senza ricalcoli e con un solo mese, privilegia il valore del blocco `Riepilogo IVA` / `Imponibile` della fornitura.
- Se il riepilogo IVA contiene voci tipo `Art. 15`, `Esclusa Art.15` o analoghe, quella quota e' esclusa dall'imponibile IVA: `imponibile_mese` deve restare il valore imponibile fiscale dichiarato nel riepilogo IVA, non la somma lorda delle macro-voci della spesa totale.
- NON sottrarre automaticamente tutta la voce `Altre partite`: sottrai solo l'eventuale quota che il PDF dichiara esplicitamente esclusa dall'imponibile IVA.
- Deve essere coerente con il periodo mensile estratto.
- Se `manca_dettaglio="no"` e il documento mostra righe economiche sufficienti, `imponibile_mese` deve essere coerente con la somma delle righe economiche incluse.
- Se NON ci sono ricalcoli e il dettaglio del mese non torna con l'imponibile fiscale, prova a ricontrollare entrambe le letture ma mantieni `imponibile_mese` uguale al valore fiscale del documento.
- Se ci sono ricalcoli o righe di altri mesi, il totale fiscale del documento puo non coincidere col solo mese corrente: in quel caso il dettaglio del mese e i flag di ricalcolo sono piu informativi del totale documento.
- Se il documento contiene ricalcoli o righe di altri mesi, usa riepilogo/imponibile solo come controllo: NON lasciare che righe di altri periodi entrino nel dettaglio del mese.
- Una tabella mensile dei consumi aiuta a ricostruire il consumo del mese, ma da sola NON implica che anche l'importo mensile sia ricostruibile.

NORMALIZZAZIONI
- Numeri: usa il punto come separatore decimale, senza separatore migliaia, mantenendo il segno.
- Date: gg/mm/aaaa.
- Testi: usa le diciture della bolletta.
- Se un campo non e' applicabile alla riga, restituisci stringa vuota.
- Compila ESATTAMENTE i campi previsti dallo schema JSON.

REGOLE FINALI
- Una riga = una voce economica mensile.
- Mantieni le righe negative.
- Se una voce esiste sia positiva sia negativa, includile entrambe.
- Non inventare righe mancanti.
- Non fare somme finali.
- Non fare distribuzioni sui mesi originari.

OUTPUT
Restituisci SOLO JSON valido con struttura:
{
  "rows": [ ... ]
}
Nessun testo fuori dal JSON.
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
                        "tipo_componente": {"type": "string"},
                        "riferimento_ricalcolo_da": {"type": "string"},
                        "riferimento_ricalcolo_a": {"type": "string"},
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
                        "presenza_ricalcolo": {"type": "string"},
                        "ricalcolo_aggregato_multi_mese": {"type": "string"},
                        "tipo_ricalcolo": {"type": "string"},
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
                        "tipo_componente",
                        "riferimento_ricalcolo_da",
                        "riferimento_ricalcolo_a",
                        "dettaglio_voce",
                        "importo",
                        "imponibile_mese",
                        "manca_dettaglio",
                        "manca_dettaglio_consumo",
                        "presenza_ricalcolo",
                        "ricalcolo_aggregato_multi_mese",
                        "tipo_ricalcolo",
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


def persist_empty_rows_debug(pdf_path: Path, raw_response: str) -> None:
    """Persist the raw model output when a PDF returns zero rows."""
    debug_dir = ROOT / "debug_empty_rows"
    debug_dir.mkdir(parents=True, exist_ok=True)
    debug_path = debug_dir / f"{pdf_path.stem}.raw_response.json"
    debug_path.write_text(raw_response, encoding="utf-8")
    logging.warning("Risposta GPT con 0 righe salvata in: %s", debug_path)


def _call_rows_prompt(
    pdf_path: Path,
    model: str,
    prompt_text: str,
    context_hint: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Call GPT with a prompt that must return rows JSON."""
    if not pdf_path.exists():
        raise FileNotFoundError(f"File non trovato: {pdf_path}")

    client = get_openai_client()
    input_file = build_pdf_input_content(pdf_path)

    prompt_with_context = prompt_text
    if context_hint:
        prompt_with_context = (
            f"{prompt_text}\n\n"
            "CONTESTO INFORMATIVO DEL RUN (solo per sanity check, non vincolante)\n"
            "- I valori seguenti arrivano da bollette gia elaborate in questo stesso run.\n"
            "- Servono solo per farti notare ordini di grandezza e coerenza numerica.\n"
            "- NON usarli per fare confronti multi-documento o per cambiare il significato del PDF corrente.\n"
            f"{context_hint}\n"
        )

    try:
        rsp = client.responses.create(
            model=model,
            instructions=(
                "Analizza il PDF e restituisci SOLO JSON valido aderente allo schema. "
                "Non aggiungere testo fuori dal JSON. "
                "Non fare calcoli finali multi-documento. "
                "Estrai solo dati del singolo documento."
            ),
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt_with_context},
                        input_file,
                    ],
                }
            ],
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
            input=[
                {
                    "role": "user",
                    "content": [
                        input_file,
                        {
                            "type": "input_text",
                            "text": (
                                f"{prompt_with_context}\n\nRESTITUISCI SOLO JSON valido con radice {{\"rows\": [...]}} "
                                f"aderente ESATTAMENTE a questo schema (nessun testo extra):\n{json.dumps(JSON_SCHEMA)}"
                            ),
                        },
                    ],
                }
            ],
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
            raw = raw[start : end + 1]

    if not raw:
        raise RuntimeError(f"Risposta vuota dal modello su: {pdf_path.name}")

    rows = parse_gpt_response(raw, pdf_path.name)
    if not rows:
        logging.warning("Il modello ha restituito 0 righe per %s", pdf_path.name)
        persist_empty_rows_debug(pdf_path, raw)
    return rows


def call_gpt_with_pdf(pdf_path: Path, model: str, context_hint: Optional[str] = None) -> List[Dict[str, Any]]:
    """Call GPT with the standard extraction prompt and return parsed rows."""
    return _call_rows_prompt(pdf_path, model, PROMPT, context_hint=context_hint)


def review_gpt_with_pdf(
    pdf_path: Path,
    model: str,
    issues: List[Dict[str, Any]],
    context_hint: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Run a second focused extraction when detail and imponibile look inconsistent."""
    issue_lines = []
    for issue in issues:
        issue_lines.append(
            "- periodo {data_inizio} -> {data_fine}: somma_righe={sum_importo}, imponibile_mese={imponibile_mese}, scarto={delta}".format(
                data_inizio=issue.get("data_inizio", ""),
                data_fine=issue.get("data_fine", ""),
                sum_importo=issue.get("sum_importo", ""),
                imponibile_mese=issue.get("imponibile_mese", ""),
                delta=issue.get("delta", ""),
            )
        )

    review_prompt = (
        f"{PROMPT}\n\n"
        "CONTROLLO QUALITA AGGIUNTIVO\n"
        "La prima estrazione ha prodotto un possibile disallineamento tra somma delle righe economiche e imponibile del periodo.\n"
        "Ricontrolla con particolare attenzione SOLO i periodi qui sotto:\n"
        f"{chr(10).join(issue_lines)}\n"
        "Regola aggiuntiva:\n"
        "- Questo controllo serve a farti correggere la prima lettura del singolo documento prima del post-processing.\n"
        "- Se il caso NON contiene ricalcoli e il dettaglio del mese e' presente, verifica che la somma delle righe economiche corrisponda all'imponibile fiscale del mese.\n"
        "- Se non torna, controlla se hai saltato una riga economica del mese oppure se hai letto male l'imponibile del riepilogo fiscale.\n"
        "- In un caso standard senza ricalcoli devi restituire l'imponibile fiscale corretto del documento e tutte le righe economiche del mese che lo compongono.\n"
        "- Per ogni mese, tieni nel dettaglio SOLO le righe che appartengono davvero a quel mese.\n"
        "- Se una riga e' un ricalcolo di altri mesi, NON farla contribuire al dettaglio del mese corrente: assegna il periodo corretto o mantienila come ricalcolo separato.\n"
        "- Se NON ci sono righe economiche sufficienti per far tornare il periodo, usa `manca_dettaglio=\"si\"` e mantieni l`imponibile_mese` corretto del periodo.\n"
        "- Se il documento contiene riepiloghi mensili o imponibili che includono altri periodi o ricalcoli, dai priorita alle righe esplicite del singolo mese.\n"
        "- Se il caso e' standard senza ricalcoli e il documento ha un `Riepilogo IVA`, ricontrolla con attenzione l`imponibile della fornitura nel riepilogo fiscale.\n"
        "- Se il riepilogo IVA riporta quote `Art. 15` o `Esclusa Art.15`, considera escluse dall'imponibile IVA solo quelle quote esplicitamente indicate: NON sottrarre tutta `Altre partite` se il PDF non lo dice.\n"
        "- NON includere automaticamente `Altre partite`: includila solo se il testo mostra che e una vera voce energetica del mese; se e una partita finanziaria/contabile come `Anticipo fornitura E.E.`, compensazione, deposito, cauzione o imposta di bollo, escludila dal dettaglio mensile.\n"
        "- Se proprio queste partite finanziarie/contabili escluse sporcano l`imponibile del mese ma il dettaglio energetico del mese resta chiaro, marca le righe energetiche del mese con `presenza_ricalcolo=\"si\"` e `tipo_ricalcolo=\"importo\"`.\n"
        "- Se il dettaglio e' completo, fai tornare la somma delle righe economiche al valore corretto del periodo.\n"
        "- Se il documento contiene anche altri mesi o ricalcoli di altri periodi, tienili separati con le loro date corrette.\n"
        "- Valorizza correttamente i flag DI RIGA `presenza_ricalcolo`, `ricalcolo_aggregato_multi_mese` e `tipo_ricalcolo`: metti `si` solo sulla riga rettificata; se il periodo copre un solo mese, `ricalcolo_aggregato_multi_mese` deve restare `no`; usa `tipo_ricalcolo=importo`, `consumo` o `importo_e_consumo` solo sulle righe davvero rettificate.\n"
        "- Restituisci di nuovo l'intero JSON finale con tutte le righe del documento.\n"
    )
    return _call_rows_prompt(pdf_path, model, review_prompt, context_hint=context_hint)
