# Bollette Energetiche Extractor

Un progetto Python modulare per l'estrazione automatica di dati strutturati da bollette energetiche (luce e gas) utilizzando l'API di OpenAI GPT.

## Cosa fa il progetto

Il progetto elabora bollette energetiche in formato PDF e ne estrae valori strutturati come:
- Metadati della bolletta (fornitore, cliente, periodi, POD/PDR, ecc.)
- Righe di dettaglio mensili (consumi, importi, voci economiche)
- Supporto per conguagli, storni e voci negative

Produce due output principali:
- **Dati grezzi**: CSV/XLSX con tutte le righe estratte da multiple bollette
- **Riepilogo mensile**: XLSX con 1 riga per mese (totale importi + consumo)

## Struttura del Progetto

```
progetto_bollette/
├── run_full_pipeline.py          # Script completo (estrazione + aggregazione)
├── src/
│   ├── extractor/
│   │   └── pdf_extractor.py      # Utility per limitare pagine PDF
│   ├── ai/
│   │   └── gpt_client.py         # Client OpenAI GPT
│   ├── parser/
│   │   └── bolletta_parser.py    # Parser JSON risposte GPT
│   └── pipeline/
│       ├── process_bolletta.py   # Script autonomo: solo estrazione
│       └── aggregate_bills.py    # Script autonomo: solo aggregazione
├── api.txt                       # API key OpenAI (opzionale)
├── requirements.txt              # Dipendenze Python
└── README.md                     # Questa documentazione
```

## Mappa del Processo

### Pipeline Completa (run_full_pipeline.py)
```
[Input: Directory con PDF Bollette]
    ↓
run_full_pipeline.py (Script autonomo completo)
    ↓
├── Estrazione da PDF (usa src/extractor, src/ai, src/parser)
│   ├── Limita pagine PDF (max 12)
│   ├── Invia a GPT-4 con prompt strutturato
│   └── Parsa JSON risposta in righe dati
│       ↓
│   Genera: estrazione_tutti_mesi.csv + .xlsx
│
└── Aggregazione mensile (logica inline)
    ├── Raggruppa per anno/mese
    ├── Calcola totale importi + consumo moda
    └── Gestisce casi "manca dettaglio"
        ↓
    Genera: bollette_raggruppate.xlsx
```

### Script Separati (src/pipeline/)
- **src/pipeline/process_bolletta.py**: Solo estrazione dati grezzi da PDF
- **src/pipeline/aggregate_bills.py**: Solo aggregazione mensile da CSV esistente

### Descrizione Componenti

- **run_full_pipeline.py**: Script principale autonomo che fa tutto il processo end-to-end
- **process_bolletta.py**: Script autonomo per estrazione. Include logica di limitazione PDF, chiamate GPT e parsing
- **aggregate_bills.py**: Script autonomo per aggregazione. Include logica di mapping colonne, conversioni e raggruppamento
- **pdf_extractor.py**: Utility per ridurre pagine PDF (max 12 per performance)
- **gpt_client.py**: Gestisce chiamate OpenAI con retry e fallback modelli
- **bolletta_parser.py**: Converte risposte JSON GPT in righe tabellari
- **file_utils.py**: Utility per API key e gestione file (non mostrato in struttura)

## Come eseguire il progetto

### Prerequisiti

- Python 3.8+
- API key OpenAI (in `api.txt` o variabile d'ambiente `OPENAI_API_KEY`)

### Installazione

1. Clona o scarica il progetto
2. Installa le dipendenze:
   ```bash
   pip install -r requirements.txt
   ```

### Esecuzione

#### Opzione 1: Pipeline Completa (Raccomandato)
```bash
python run_full_pipeline.py /path/to/pdf/directory
```
Elabora PDF → estrae dati → aggrega mensilmente → genera tutti gli output.

#### Opzione 2: Script Separati (Flessibilità)
Se vuoi eseguire solo parti specifiche:

**Solo Estrazione:**
```bash
python src/pipeline/process_bolletta.py
python src/pipeline/process_bolletta.py data
python src/pipeline/process_bolletta.py /path/to/pdf/directory
```
Se ometti `input_dir`, lo script cerca prima `data/` nella root del progetto e poi `tests/data/`.

→ Genera `estrazione_tutti_mesi.csv` + `.xlsx`

**Solo Aggregazione:**
```bash
python src/pipeline/aggregate_bills.py
python src/pipeline/aggregate_bills.py estrazione_tutti_mesi.csv bollette_raggruppate.xlsx
```
Se ometti gli argomenti, lo script usa `estrazione_tutti_mesi.csv` e `bollette_raggruppate.xlsx` nella root del progetto.

→ Genera `bollette_raggruppate.xlsx`

### Configurazione

- Modifica `MODEL_PRIMARY` e `MODEL_FALLBACK` in `src/ai/gpt_client.py` per cambiare modello GPT
- Regola `MAX_PAGES` in `src/extractor/pdf_extractor.py` per limitare pagine PDF
- Parametri pipeline (retry, sleep) nei file script (run_full_pipeline.py, process_bolletta.py)

## Esempio di Output

Vedi `examples/example_output.json` per un esempio della struttura JSON estratta.

Il CSV finale contiene colonne come:
- nome_cliente, pdr/pod, data_inizio/fine, consumo_totale, dettaglio_voce, importo, imponibile_mese, ecc.

Il file `bollette_raggruppate.xlsx` contiene un riepilogo aggregato per mese:
- anno, mese, totale_importi (somma o imponibile), consumo_mese (moda), ecc.

## Limitazioni

- Non gestisce ricalcoli/conguagli senza dettaglio (prende imponibile aggregato)
- Bollette multiple accorpate in un PDF potrebbero non essere gestite correttamente
- Richiede API OpenAI con crediti sufficienti
- PDF con header non standard (BOM) possono produrre warning ma vengono elaborati correttamente

## Test

Esegui i test con:
```bash
python -m unittest tests/test_pipeline.py
```
*Nota: I test potrebbero essere obsoleti dopo la rifattorizzazione degli script autonomi.*

## Sicurezza

- L'API key può essere in `api.txt` o `OPENAI_API_KEY` env var
- Non loggare dati sensibili delle bollette

## Changelog Recente

- **v2.0**: Rifattorizzazione completa
  - Script autonomi senza dipendenze incrociate
  - `run_full_pipeline.py` nella root per uso completo
  - `src/pipeline/process_bolletta.py` e `aggregate_bills.py` per componenti separate
  - Rimossi moduli intermedi duplicati
  - Documentazione aggiornata
