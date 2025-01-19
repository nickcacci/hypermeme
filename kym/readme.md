# Sistema di Scraping e Indicizzazione Memes

## Descrizione Generale

Questo sistema esegue lo scraping di meme dal sito **Know Your Meme (KYM)**, salvando i dati rilevanti e indicizzando immagini e metadati su **Elasticsearch** per consentire ricerche basate sulla similarità delle immagini.

Il sistema si compone di diverse fasi:

1. **Scraping dei meme**: Recupero delle informazioni dettagliate relative ai meme.
2. **Download delle immagini**: Salvataggio delle immagini di esempio e dei template.
3. **Calcolo embedding**: Generazione degli embedding delle immagini usando **OpenCLIP**.
4. **Indicizzazione**: Invio dei dati a Elasticsearch per l'archiviazione e la ricerca.
5. **Prevenzione duplicati**: Memorizzazione dei meme già elaborati in un file JSON locale.

---

## Struttura del Sistema

### Configurazione

- **Base URL**: `https://knowyourmeme.com`
- **Cartella Output**: `./memes_dataset`
- **File Dati Scraping**: `./scraped_data.json`
- **Modello OpenCLIP**: `ViT-B-32` (preaddestrato su OpenAI)
- **Elasticsearch**: configurato per il caricamento dei metadati e degli embedding delle immagini.

### Componenti Principali

#### 1. **Scraping dei Meme**

Il sistema parte da una pagina di elenco ordinata per visualizzazioni o cronologia:

- URL di partenza: `https://knowyourmeme.com/memes?sort=views`
- Recupero dei link di ciascun meme dall'elenco.
- Parsing della pagina del meme per estrarre:
  - **Sezione About**: descrizione dettagliata.
  - **Anno**: anno di origine del meme.
  - **Tag**: etichette associate al meme.
  - **Template**: immagini template correlate.

#### 2. **Download delle Immagini**

Per ogni meme:

- Le immagini template vengono scaricate nella cartella `./memes_dataset/<nome-meme>/templates`.
- I file salvati seguono il formato: `template-<indice>-<nome-meme>.<estensione>`.

#### 3. **Calcolo degli Embedding**

- Le immagini vengono preprocessate e passate attraverso il modello **OpenCLIP** per ottenere embedding vettoriali utilizzati per ricerche di similarità.

#### 4. **Indicizzazione su Elasticsearch**

- **Indice dei metadati** (`meme_metadata`):
  - Nome del meme.
  - Sezione "About".
  - Anno e tag.
- **Indice dei template** (`meme_templates`):
  - Percorso locale del file.
  - Embedding CLIP.

#### 5. **Prevenzione duplicati**

- Un file JSON (`scraped_data.json`) registra i meme già elaborati.
- Ogni meme già presente nel file viene saltato nelle esecuzioni successive.

---

## Workflow del Sistema

### Passaggi Principali

1. **Caricamento dei Dati Preesistenti**:
   - Controlla `scraped_data.json` per individuare i meme già elaborati.
2. **Recupero della Lista dei Meme**:
   - Effettua il parsing della pagina principale per ottenere i link ai singoli meme.
3. **Elaborazione dei Singoli Meme**:
   - Recupera le informazioni rilevanti (about, anno, tag, template).
   - Scarica le immagini template.
   - Calcola gli embedding delle immagini.
   - Indicizza i metadati e i template in Elasticsearch.
4. **Aggiornamento del File JSON**:
   - Aggiunge il meme elaborato a `scraped_data.json` per evitare duplicazioni.

### Riepilogo Struttura Elasticsearch

- **Indice: `meme_metadata`**
  - Campi:
    - `meme_name` (stringa)
    - `about` (testo)
    - `year` (stringa)
    - `tags` (array di stringhe)
- **Indice: `meme_templates`**
  - Campi:
    - `meme_name` (stringa)
    - `template_path` (stringa)
    - `clip_embedding` (array di float)

---

## Requisiti Tecnici

### Dipendenze

- **Librerie Python**:
  - `requests`
  - `beautifulsoup4`
  - `pillow`
  - `open_clip`
  - `torch`
  - `elasticsearch`
  - `logging`
- **Servizi**:
  - Elasticsearch (eseguibile su `http://localhost:9200`)

### Esecuzione del Sistema

1. Configurare Elasticsearch e assicurarsi che sia attivo.
2. Eseguire il programma Python:

   ```bash
   python <nome_script>.py
   ```

3. Verificare che i dati siano indicizzati correttamente nei due indici configurati.

---

## Possibili Miglioramenti

- **Caching avanzato**: utilizzo di un database per sostituire `scraped_data.json`.
- **Gestione errori**: implementare meccanismi di retry per il download delle immagini e l'accesso a KYM.
- **Supporto per aggiornamenti incrementali**: aggiungere nuove funzionalità per rilevare e indicizzare i nuovi contenuti senza duplicazioni.
- **Ricerca visuale avanzata**: aggiungere una GUI per caricare un'immagine e cercare direttamente i meme correlati.

---

## Conclusione

Questo sistema consente di costruire un dataset di meme ricco e indicizzato, utile per ricerche basate sulla similarità delle immagini e per integrare informazioni contestuali tramite analisi dei template e dei metadati associati.
