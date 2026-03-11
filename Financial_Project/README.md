dati grezzi → dati puliti → rendimenti → stime → decisione → valutazione

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

## Struttura logica

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


