flowchart TB
    %% Subgraph 5: Data Ingestion
    subgraph Ingestion ["5\. Data Ingestion"]
        A["5.1 Meme da fonti diverse"]
        B["5.2 Upload meme tramite webapp"]
    end

    %% Subgraph 1 & 2: Pipeline di Analisi e Arricchimento
    subgraph MainPipeline ["Pipeline di Analisi e Arricchimento"]
        C["1\. Vision LLM<br>(Input: Immagine + Prompt, Output: Arricchimento)"]
        D["2\. Modello per Embeddings<br>(Calcolo embeddings di testo e immagine)"]
        
        subgraph Enrichment ["Arricchimento del Dato"]
            E["1a. Structured Output"]
            F["2a. Embeddings Testo"]
            G["2b. Embeddings Immagine"]
        end
    end



    %% Subgraph 3 & 4: Archiviazione
    subgraph Storage ["Sistema di Archiviazione"]
        H["3\. Vector Database<br>(Archivia dati e metadati con supporto vettoriale)"]
        I["4\. Archiviazione Immagini"]
    end

    %% Subgraph 6 & 7: Interfacce Utente
    subgraph Frontend ["Interfacce Utente"]
        K["6\. Web App<br>Visualizza ricerca e carica meme"]
        L["7\. Dashboard Interattiva<br>Statistiche e analisi dell'archivio"]
    end

    %% Connessioni tra i nodi
    Ingestion -- "Immagine + Metadati" --> C
    Ingestion -- "Immagine" --> I
    Ingestion -- "Immagine" --> D

    C --> E
    E --> D
    D --> F & G

    E --> H
    F --> H
    G --> H

    H -- "URL locale" --> I

    I --> K
    H --> K
    H --> L

    %% Stili
    style Ingestion fill:#e6f3ff,stroke:#333,stroke-width:2px
    style MainPipeline fill:#fff3e6,stroke:#333,stroke-width:2px
    style Storage fill:#e6ffe6,stroke:#333,stroke-width:2px
    style Frontend fill:#ffe6e6,stroke:#333,stroke-width:2px
