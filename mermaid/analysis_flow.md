flowchart TB
    %% Pipeline di Ingestion, Analisi e Arricchimento dei dati
    subgraph Ingestion ["1\. Ingestion"]
        A["Meme immesso nel sistema<br>(indipendentemente dalla sorgente)"]

    end

    subgraph Analysis ["2\. Arricchimento tramite Vision LLM"]
        C["Input: Immagine + Metadati<br>(Titolo, Data, ecc.)"]
        D["Vision LLM<br>(Analizza e arricchisce il meme)"]
        subgraph Enrichment ["Output strutturato"]
            E["Descrizione del contenuto visuale"]
            F["Testo estratto"]
            G["Spiegazione del significato"]
        end
    end

    %% Collegamenti
    Ingestion --> Analysis
    C --> D
    D --> E
    D --> F
    D --> G

    %% Stili
    style Ingestion fill:#e6f3ff,stroke:#333,stroke-width:2px
    style Analysis fill:#fff3e6,stroke:#333,stroke-width:2px
