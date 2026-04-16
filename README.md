# Bollette Energetiche Extractor

Progetto Python per:
- leggere bollette PDF una alla volta via API OpenAI
- salvare l'estrazione strutturata in CSV/XLSX
- aggregare i dati per mese di competenza
- esporre tutto anche tramite una web interface interna

## Cosa produce

La pipeline genera 3 report:
- `estrazione_tutti_mesi.csv`
- `estrazione_tutti_mesi.xlsx`
- `bollette_raggruppate.xlsx`

Nel file aggregato e presente anche il foglio `warning_mesi`, utile per capire dove l'automazione e affidabile e dove conviene una verifica manuale.

## Stato attuale del progetto

Il progetto oggi e organizzato in 2 step distinti:

1. Estrazione documento per documento
- ogni PDF viene inviato singolarmente a GPT
- GPT non vede le altre bollette
- GPT restituisce righe economiche, campi consumo e segnali per i ricalcoli aggregati

2. Post-processing finale
- tutte le righe vengono aggregate insieme
- importi e consumi vengono ricostruiti per mese
- i ricalcoli aggregati vengono ripartiti solo qui, non in fase di estrazione

## Struttura progetto

```text
Progetto_bollette/
|-- run_full_pipeline.py
|-- run_web_interface.py
|-- requirements.txt
|-- api.txt
|-- README.md
|-- src/
|   |-- ai/
|   |   |-- __init__.py
|   |   `-- gpt_client.py
|   |-- extractor/
|   |   |-- __init__.py
|   |   `-- pdf_extractor.py
|   |-- parser/
|   |   |-- __init__.py
|   |   `-- bolletta_parser.py
|   |-- pipeline/
|   |   |-- __init__.py
|   |   |-- process_bolletta.py
|   |   `-- aggregate_bills.py
|   |-- utils/
|   |   |-- __init__.py
|   |   `-- file_utils.py
|   `-- web/
|       |-- __init__.py
|       |-- app.py
|       |-- static/
|       |   `-- app.css
|       `-- templates/
|           `-- index.html
`-- tests/
    |-- test_gpt_client.py
    |-- test_pipeline.py
    |-- test_aggregate_bills.py
    `-- test_run_full_pipeline.py
```

## Componenti principali

### `run_full_pipeline.py`
Entry point completo.

Fa:
- ricerca PDF input
- estrazione tramite `process_bolletta.py`
- aggregazione finale tramite `aggregate_bills.py`
- restituzione dei path dei 3 report

### `src/pipeline/process_bolletta.py`
Step di sola estrazione.

Fa:
- risoluzione cartella input
- filtro PDF case-insensitive
- taglio PDF a massimo 12 pagine
- chiamata GPT
- parsing JSON
- salvataggio `estrazione_tutti_mesi.csv` e `.xlsx`

### `src/pipeline/aggregate_bills.py`
Step di sola aggregazione.

Fa:
- lettura CSV/XLSX estratto
- ricostruzione importi mensili
- ricostruzione consumi mensili
- gestione ricalcoli aggregati
- generazione warning finali
- export `bollette_raggruppate.xlsx`

### `src/ai/gpt_client.py`
Client OpenAI.

Fa:
- costruzione prompt
- upload PDF in base64
- chiamata Responses API
- validazione tramite JSON schema
- salvataggio diagnostico in `debug_empty_rows/` quando GPT restituisce 0 righe

### `src/web/app.py`
Web app Flask interna.

Fa:
- upload PDF multiplo con drag and drop
- esecuzione pipeline
- download dei 3 report
- visualizzazione warning finali
- storico run in `web_runs/`

## Modello usato

Di default:
- `MODEL_PRIMARY = gpt-5`
- `MODEL_FALLBACK = gpt-5`

Puoi cambiare il modello impostando la variabile ambiente:

```powershell
$env:OPENAI_MODEL="gpt-5"
```

oppure modificando direttamente [gpt_client.py](/c:/Users/sdigiammarino/OneDrive%20-%20energonesco.it/Documenti/progetti_energon/Progetto_bollette/src/ai/gpt_client.py).

## Limite pagine PDF

I PDF vengono limitati a:
- `MAX_PAGES = 12`

File:
- [pdf_extractor.py](/c:/Users/sdigiammarino/OneDrive%20-%20energonesco.it/Documenti/progetti_energon/Progetto_bollette/src/extractor/pdf_extractor.py)

Se il PDF ha piu di 12 pagine, viene creato un temporaneo con le prime 12.

## Installazione

### Requisiti

- Python 3.10+ consigliato
- chiave OpenAI valida

### Dipendenze

```powershell
python -m pip install -r requirements.txt
```

### Ambiente virtuale consigliato

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Configurazione API key

Il progetto cerca la chiave in questo ordine:
- variabile ambiente `OPENAI_API_KEY`
- file `api.txt`
- prompt interattivo

Formato minimo di `api.txt`:

```text
sk-...
```

## Esecuzione da terminale

### Pipeline completa

```powershell
python .\run_full_pipeline.py .\tests\data
```

### Solo estrazione

```powershell
python .\src\pipeline\process_bolletta.py
python .\src\pipeline\process_bolletta.py .\data
python .\src\pipeline\process_bolletta.py .\tests\data
```

Se non passi la cartella input, lo script cerca:
- `data/`
- poi `tests/data/`

### Solo aggregazione

```powershell
python .\src\pipeline\aggregate_bills.py
python .\src\pipeline\aggregate_bills.py .\estrazione_tutti_mesi.csv .\bollette_raggruppate.xlsx
```

## Web interface

### Avvio consigliato con Flask

Da root progetto:

```powershell
$env:FLASK_APP="run_web_interface.py"
flask run --host=0.0.0.0 --port=8000
```

In alternativa funziona ancora anche:

```powershell
python .\run_web_interface.py
```

### URL locale

Apri poi:

```text
http://127.0.0.1:8000/Lettura_Bollette_Energon
```

### URL in rete interna

Se la pagina gira su un server o PC raggiungibile dai dipendenti in LAN, l'URL diventa:

```text
http://<nome-server-o-ip>:8000/Lettura_Bollette_Energon
```

Il path applicativo e configurato in:
- [app.py](/c:/Users/sdigiammarino/OneDrive%20-%20energonesco.it/Documenti/progetti_energon/Progetto_bollette/src/web/app.py)

e di default vale:

```text
/Lettura_Bollette_Energon
```

Puoi cambiarlo impostando la variabile ambiente:

```powershell
$env:ENERGON_WEB_PATH="/Lettura_Bollette_Energon"
```

Funzioni disponibili:
- upload PDF multiplo
- drag and drop
- pulsante `Avvio` che esegue `run_full_pipeline.py`
- download dei 3 report
- warning finali per mese

Output dei run web:
- cartella `web_runs/<job_id>/input`
- cartella `web_runs/<job_id>/output`
- manifest `web_runs/<job_id>/result.json`

### Nota per la pubblicazione interna

Il progetto e ora pronto per essere esposto in rete interna a livello applicativo:
- il server ascolta su `0.0.0.0`
- il path pubblico e `Lettura_Bollette_Energon`
- `run_web_interface.py` espone l'oggetto `app` per `flask run`

Per renderlo davvero accessibile ai dipendenti devi comunque farlo girare su:
- un PC sempre acceso
- oppure un server interno Windows/Linux
- con porta `8000` aperta in rete locale

## Campi estratti principali

Esempi di colonne nel file estratto:
- `_source_file`
- `nome_cliente`
- `pod`
- `pdr`
- `data_inizio`
- `data_fine`
- `consumo_totale`
- `consumo_dettaglio_riga`
- `tipo_componente`
- `blocco_ricalcolo_aggregato`
- `riferimento_ricalcolo_da`
- `riferimento_ricalcolo_a`
- `ricalcolo_spalmabile`
- `manca_dettaglio_ricalcolo`
- `dettaglio_voce`
- `importo`
- `imponibile_mese`
- `manca_dettaglio`
- `manca_dettaglio_consumo`

## Logica di aggregazione attuale

### Importi

Per ogni mese:
- se il dettaglio economico e ricostruibile, usa `sum(importo)`
- se il dettaglio manca, usa `imponibile_mese`
- se ci sono ricalcoli aggregati, li aggiunge sopra la base del mese

### Consumi

Per ogni mese:
- se esiste vero dettaglio consumo, usa `sum(consumo_dettaglio_riga)`
- se il dettaglio consumo manca, usa `consumo_totale`
- se GPT ha copiato la stessa quantita su piu righe importi come base tariffaria, il sistema lo tratta come dettaglio non ricostruibile e torna a `consumo_totale`

Questa protezione evita casi come:
- `5013 + 5013 = 10026`

quando il PDF in realta mostra solo una quantita di mese ripetuta su due righe economiche.

### Ricalcoli aggregati

La ripartizione avviene solo in `aggregate_bills.py`.

Regole:
- `fissa` -> divisione uniforme per il numero di mesi
- `variabile` -> ripartizione proporzionale ai consumi mensili
- non classificabile -> fallback come `fissa`, con warning

Il ricalcolo viene sommato al valore base del mese.

### Warning finali

Nel file aggregato compaiono colonne come:
- `warning_mese`
- `warning_count`
- `metodi_ripartizione_ricalcolo`
- `source_file_elenco`
- `consumo_logica_usata`
- `importo_logica_usata`

E viene generato anche il foglio Excel:
- `warning_mesi`

## Regole importanti del prompt GPT

Il prompt attuale e stato irrigidito su questi punti:
- GPT vede una sola bolletta alla volta
- non deve fare ragionamenti multi-documento
- non deve spalmare i ricalcoli
- `consumo_dettaglio_riga` va usato solo per vero dettaglio consumo
- se la quantita e solo ripetuta sulle righe importi, allora NON e dettaglio consumo
- `manca_dettaglio` e `manca_dettaglio_consumo` sono flag documentali, non di singola riga
- more e preventivi futuri vanno esclusi

## File di output

### `estrazione_tutti_mesi.csv`
Uso operativo e debug rapido.

### `estrazione_tutti_mesi.xlsx`
Versione Excel dell'estrazione.

### `bollette_raggruppate.xlsx`
Report mensile finale con:
- `riepilogo_mesi`
- `warning_mesi`

## Debug e diagnostica

Se GPT restituisce zero righe per un PDF, il raw output viene salvato in:

```text
debug_empty_rows/
```

Questo aiuta a capire se il problema nasce:
- dal prompt
- dallo schema JSON
- dalla risposta del modello

## Test

Esegui tutta la suite:

```powershell
python -m unittest tests\test_gpt_client.py tests\test_aggregate_bills.py tests\test_pipeline.py tests\test_run_full_pipeline.py
```

## Limitazioni note

- GPT puo ancora classificare male alcuni ricalcoli aggregati
- il dettaglio consumo dipende molto da come il PDF espone le righe
- se manca del tutto la bolletta base di un mese e rimane solo un ricalcolo successivo, l'aggregato di quel mese avra solo la quota ricalcolata
- lavorando dentro OneDrive, i file possono risultare piu esposti a conflitti di sincronizzazione o cancellazioni/sovrascritture accidentali

## Troubleshooting rapido

### `ModuleNotFoundError: No module named 'flask'`

```powershell
python -m pip install -r requirements.txt
```

### `Directory input non trovata`
Controlla di essere nella root progetto oppure passa il path completo.

### `0 righe estratte da un PDF`
Controlla:
- `debug_empty_rows/`
- il prompt in `src/ai/gpt_client.py`
- il PDF originale e il suo dettaglio mensile

### `consumo_mese` troppo alto
Controlla se `consumo_dettaglio_riga` e stato copiato sulle righe importi come base tariffaria. La versione corrente prova gia a neutralizzare questo caso.

## Esempi utili

Pipeline completa su dati test:

```powershell
python .\run_full_pipeline.py .\tests\data
```

Web interface:

```powershell
python .\run_web_interface.py
```

Solo aggregazione su un CSV gia prodotto:

```powershell
python .\src\pipeline\aggregate_bills.py .\estrazione_tutti_mesi.csv .\bollette_raggruppate.xlsx
```
