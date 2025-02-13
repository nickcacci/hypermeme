flowchart TB
    %% Indicizzazione dei dati
    A["Campi arricchiti + Immagine del meme"]
    subgraph Indexing ["Rappresentazione"]

        B["3\. Modello di Embedding<br>(Testo e immagine nello stesso spazio vettoriale)"]
    end

    A --> B
    B --> DB

    %% Nodo Database (rappresentato in forma di database)
    DB[("4\. Salvataggio in Database<br>con supporto ai vettori")]

    %% Ricerca Semantica tramite WebApp
    subgraph Search ["Ricerca Semantica"]
        D["5\. Utente tramite WebApp 🔍"]
        E["Ricerca per Testo<br>(Similarità sui campi testuali) 🔍"]
        F["Ricerca per Immagine<br>(Confronto degli embeddings) 🔍"]
    end

    %% Collegamenti bidirezionali tra il Database e i nodi di ricerca

    D --> E
    D --> F
    E <--> DB
    F <--> DB

    %% Stili
    style Indexing fill:#e6ffe6,stroke:#333,stroke-width:2px
    style Search fill:#ffe6e6,stroke:#333,stroke-width:2px
    style DB fill:#ffe6ff,stroke:#333,stroke-width:2px
