dati grezzi → dati puliti → rendimenti → stime → decisione → valutazione
Quindi definire quali sono gli elementi che vogliamo inserire
Probabilmente definire un paniere di titoli interessanti, magari in settori considerabili "nobili potrebbe essere una buona idea.
Io pensavo ad un paniere di titoli del settore agricolo, per la produzione alimentare, ecc...
Applicare la teoria finanziaria a queste tipologie di titoli, e vedere magari come questi si possono inserire all'interno di un portafoglio d'investimento piuttosto complesso, come quello che può essere se composto da titoli privi di rischio e altro.
Confrontare magari diverse tipologie di portafogli, basate se diverse tipologie di 
avversione al rischio. Se non sbaglio erano 7 effettivamente i livelli di investimento che si possono 
prevedere, e si potrebbe fare un'analisi di tutti questi costruendo diverse tipologie di portafogli ottimi. 

Evidenziare diverse tipologie di indici che possano spiegare o meno l'andamento e la performance del
portafoglio rispetto agli altri e ai titoli dei vari soggetti, anche presi singolarmente. 

Evitare di riscaricare e di "tenere in memoria" molti dati secondo me è un punto fondamentale, magari tenere solo medie o indicatori di dati passati (anche se rilevanti) può alleggerrire il tutto

Tenere a mente una buona organizzazione del flusso di lavoro per risparmiare tempo 

# Librerie: poche ma giuste
Per una prima versione ben difendibile, io terrei un set essenziale:
- pandas per tabelle e serie temporali
- numpy per calcoli numerici
- una fonte dati semplice come Yahoo Finance o CSV locali
- matplotlib per grafici
- opzionalmente PyPortfolioOpt per confrontare i tuoi risultati con una libreria già strutturata



# Architettura del progetto: portfolio quantitativo in Python

## Obiettivo
Costruire un progetto Python ben organizzato, veloce da eseguire e facile da spiegare, che faccia:
- download e pulizia dati finanziari
- calcolo rendimenti e indicatori di rischio/rendimento
- ottimizzazione di portafoglio
- backtest semplice di strategie
- visualizzazioni chiare

## Principi guida
1. Separare acquisizione dati, logica finanziaria, visualizzazione e avvio del programma.
2. Evitare notebook monolitici.
3. Tenere poche dipendenze, ma scelte bene.
4. Rendere ogni modulo testabile da solo.
5. Partire semplice e poi aggiungere complessità.

## Struttura logica (TROPPO DISPERSIVA?)

```text
project/
├── data/
│   ├── raw/
│   └── processed/
├── notebooks/
│   └── exploratory_analysis.ipynb
├── src/
│   └── quant_project/
│       ├── __init__.py
│       ├── config.py
│       ├── data_loader.py
│       ├── preprocessing.py
│       ├── metrics.py
│       ├── indicators.py
│       ├── optimizer.py
│       ├── backtest.py
│       ├── visualization.py
│       └── app.py
├── tests/
├── main.py
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Ruolo dei moduli
- `data_loader.py`: scarica o legge dati da file.
- `preprocessing.py`: pulizia, allineamento date, rendimenti, gestione NaN.
- `metrics.py`: volatilità, Sharpe ratio, drawdown, CAGR.
- `indicators.py`: medie mobili, RSI, rolling volatility.
- `optimizer.py`: Markowitz, minimum variance, max Sharpe.
- `backtest.py`: simulazione pesi e performance nel tempo.
- `visualization.py`: grafici prezzi, rendimenti cumulati, efficient frontier.
- `config.py`: parametri centrali del progetto.
- `app.py` / `main.py`: orchestration dell'esecuzione.

## Flusso dati
1. Download dati
2. Pulizia e standardizzazione
3. Calcolo ritorni
4. Costruzione metriche e indicatori
5. Ottimizzazione portafoglio o segnali strategici
6. Backtest
7. Grafici e report finale

## Versione 1 consigliata
Per la prima versione:
- 3-6 asset
- dati giornalieri
- expected returns storici
- matrice di covarianza storica
- portafoglio equally weighted vs minimum variance vs max Sharpe
- grafico dei prezzi normalizzati
- grafico dei rendimenti cumulati
- tabella finale con rendimento, volatilità, Sharpe e max drawdown

## Roadmap
### Fase 1
Definizione obiettivo e architettura.

### Fase 2
Implementazione parte dati e preprocessing.

### Fase 3
Metriche di performance.

### Fase 4
Ottimizzazione portafoglio.

### Fase 5
Backtest e confronto strategie.

### Fase 6
Pulizia finale, README e presentazione.

## Criterio di successo
Il progetto deve essere:
- corretto finanziariamente
- ordinato dal punto di vista software
- rapido da eseguire
- facile da spiegare all'esame
- estendibile in futuro

## Nel tuo caso, visto che vuoi capire davvero quello che fai e non complicarti la vita inutilmente, io farei una distinzione molto chiara tra:
- architettura finale ideale
- architettura minima realistica da implementare adesso
- E questa è la cosa più importante da fissare

# Per il momento mi risperbo di fare questa struttura, poi vediamo:
```text
project/
│
├── data/
│   ├── raw/
│   └── processed/
├── notebooks/
│   └── exploratory_analysis.ipynb
├── src/
│   ├── universe.py
│   ├── data_loader.py
│   ├── preprocessing.py
│   ├── portfolio.py
│   ├── backtest.py
│   ├── visualization.py
│   └── app.py
├── main.py
├── requirements.txt
└── README.md
```
### Ruolo di ciascun file:
- data_loader.py: scarica o legge i prezzi
- preprocessing.py: pulisce dati, allinea date, calcola returns base
- portfolio.py: calcola pesi, metriche, portafogli confronto
- backtest.py: simula andamento dei portafogli nel tempo
- visualization.py: fa i grafici
- app.py: coordina il flusso
- main.py: avvia tutto

## Separazione dei grafici dai calcoli, non mischiare quindi matematica e presentazione


https://pyportfolioopt.readthedocs.io/en/latest/ExpectedReturns.html

https://packaging.python.org/en/latest/tutorials/packaging-projects/?utm=

https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/?utm=

https://docs.pytest.org/en/stable/explanation/goodpractices.html?utm=

https://pandas-datareader.readthedocs.io/en/latest/remote_data.html

## Roadmap di sviluppo del progetto

Per evitare di scrivere codice in modo disordinato e poco controllabile, il progetto viene sviluppato seguendo una sequenza logica precisa.  
Ogni fase serve a chiarire un aspetto fondamentale prima di passare alla successiva.

### Fase 1: definire con precisione il progetto finale

Questa è la fase più importante, perché definisce chiaramente cosa il progetto deve fare e cosa invece rimane fuori.

In questa fase si stabiliscono:

- l’obiettivo principale del progetto
- il tipo di dati utilizzati
- il numero di asset analizzati
- il tipo di portafogli da confrontare
- le metriche di performance da calcolare
- gli output finali (grafici, tabelle, risultati numerici)

L'obiettivo del progetto è costruire un'applicazione Python per l'analisi quantitativa di portafoglio che:

- acquisisce dati finanziari storici
- costruisce diversi portafogli comparabili
- esegue un backtest storico
- calcola metriche di rischio e rendimento
- visualizza i risultati tramite grafici e un'interfaccia interattiva

Per mantenere il progetto realistico e ben controllato, alcune componenti avanzate vengono volutamente escluse nella prima versione, come:

- modelli di machine learning
- LSTM o modelli predittivi complessi
- dati intraday ad alta frequenza
- sistemi di trading live

---

### Fase 2: decidere il flusso dati completo

In questa fase viene definita la pipeline dei dati, cioè il percorso che i dati seguono dall'acquisizione iniziale fino ai risultati finali.

Il flusso dati del progetto segue questa sequenza logica:

1. download dei dati finanziari
2. pulizia e standardizzazione dei dati
3. calcolo dei rendimenti
4. costruzione delle metriche di rischio e rendimento
5. costruzione dei portafogli
6. simulazione tramite backtest
7. produzione di grafici e risultati finali

Questa pipeline permette di mantenere il progetto ordinato e garantisce che ogni passaggio del processo sia chiaramente identificabile.

---

### Fase 3: fissare cartelle e moduli

Una volta definito il flusso dei dati, il passo successivo consiste nel tradurre la logica del progetto in una struttura di cartelle e moduli Python.

Ogni parte del processo viene associata a un modulo specifico del progetto.

Ad esempio:

- acquisizione dei dati → `data_loader.py`
- pulizia e preparazione dei dati → `preprocessing.py`
- costruzione dei portafogli → `portfolio.py`
- simulazione delle performance → `backtest.py`
- visualizzazione dei risultati → `visualization.py`

Questa separazione consente di mantenere il codice modulare e più facile da comprendere e mantenere.

---

### Fase 4: definire cosa entra e cosa esce da ogni file

In questa fase si stabilisce in modo preciso quali dati ogni modulo riceve in input e quali risultati restituisce in output.

Questo passaggio è fondamentale per evitare che i diversi file del progetto diventino troppo dipendenti tra loro o che alcune funzioni svolgano troppi compiti contemporaneamente.

Esempi di flusso tra moduli:

- `data_loader.py`
  - input: ticker, data di inizio, data di fine
  - output: DataFrame contenente i prezzi storici

- `preprocessing.py`
  - input: DataFrame dei prezzi
  - output: DataFrame dei rendimenti e dati puliti

- `portfolio.py`
  - input: rendimenti storici
  - output: pesi del portafoglio e metriche di base

- `backtest.py`
  - input: rendimenti e pesi del portafoglio
  - output: serie temporale della performance del portafoglio

- `visualization.py`
  - input: prezzi, rendimenti e performance del portafoglio
  - output: grafici e rappresentazioni visive dei risultati

Questa fase garantisce che ogni componente del progetto abbia un ruolo chiaro e ben definito.

---

### Fase 5: solo allora iniziare a scrivere codice

Solo dopo aver definito:

- l'obiettivo del progetto
- il flusso dei dati
- la struttura delle cartelle
- gli input e gli output di ogni modulo

si procede con l'implementazione del codice.

Seguire questo approccio permette di evitare codice disordinato e garantisce che l'intero progetto rimanga coerente, leggibile e facilmente estendibile.

# Per il momento solo BORSA ITALIANA, escludiamo ETC, ETN, Derivati e Certificati.
## A 5 anni, e partiamo con una tabella anagrafica degli strumenti.
| ticker | name            | type   |
| ------ | --------------- | ------ |
| ENI    | Eni             | equity |
| ISP    | Intesa Sanpaolo | equity |
| ...    | ...             | etf    |
| ...    | ...             | bond   |
| ...    | ...             | fund   |
### Parto a fare questo in universe.py
