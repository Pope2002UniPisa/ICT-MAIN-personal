# Sitografia interessante:
- https://it.wikipedia.org/wiki/Process_mining#cite_note-9
- https://www.processmining.org/      (NOTA BENE)
- https://sourceforge.net/projects/prom/
- https://sourceforge.net/projects/promimport/
- https://it.wikipedia.org/wiki/Data_mining
- https://it.wikipedia.org/wiki/Business_process_modeling
- https://www.ibm.com/it-it/products/streamsets?utm_content=SRCWW&p1=Search&p4=359104816748&p5=b&p9=182804128202&gclsrc=aw.ds&gad_source=1&gad_campaignid=22698797891&gbraid=0AAAAA-h2TOERLucLzv7f43ygLwW_50TAs&gclid=CjwKCAjw1tLOBhAMEiwAiPkRHnE2vcVCBq0DgaVwLBaGfLWet0zZg5U4ydt60aTkkDHk42eMZDbyeBoC9zsQAvD_BwE
- https://it.wikipedia.org/wiki/Rete_di_Petri
- https://www.youtube.com/watch?v=5thuFbUQ7Qg
- https://en.wikipedia.org/wiki/Alpha_algorithm
- https://bi-rex.it/process-mining-efficienza-compliance-automazione/#
- https://www.math.unipd.it/~deleoni/documenti/MondoDigitale.pdf    (pdf)
- https://www.tf-pm.org/
- https://www.xes-standard.org/
- https://ieeexplore.ieee.org/document/10267858
- https://www.tf-pm.org/resources/manifesto       (manifesto process mining IEEE)
- https://mpmx.com/process-intelligence-platform?utm_term=software%20process%20mining&utm_campaign=%5BmpmX%5D+-+Process+Mining+-+EU+EN+-+Generic&utm_source=adwords&utm_medium=ppc&hsa_acc=9154849069&hsa_cam=21868824847&hsa_grp=170210220936&hsa_ad=720311535100&hsa_src=g&hsa_tgt=kwd-608122734124&hsa_kw=software%20process%20mining&hsa_mt=p&hsa_net=adwords&hsa_ver=3&gad_source=1&gad_campaignid=21868824847&gbraid=0AAAAADpe4FvPO2kTj03s6xG2GfTzEItEs&gclid=CjwKCAjw-dfOBhAjEiwAq0RwI4bmvXOhBR6o52Y09HKIRhkxfMJzjM6r6ofM_iJnGcgXznbAwhEAExoCbTcQAvD_BwE
- https://www.youtube.com/watch?v=HEb_ukW7rEw
- https://ais.win.tue.nl/coselog/wiki/index.html
- https://www.diag.uniroma1.it/deluca/automation/Automazione_RetiPetri.pdf     (pdf)
- https://eprints.qut.edu.au/74865/1/bpiChallenge.pdf   (pdf)


# Bibliografia interessante:
- Aalst, W. van der, Weijters, A., & Maruster, L. (2004) - Workflow Mining: Discovering Process Models from Event Logs - IEEE Transactions on Knowledge and Data Engineering, 16 (9), 1128-1142.
- Process Mining - Come estrarre conoscenza dai log dei processi di business, Wil M.P. van der Aalst, Andrea Burattin, Massimiliano de Leoni, Antonella Guzzo, Fabrizio M. Maggi e Marco Montali. 
- Process Mining Manifesto - IEEE Task Force on Process Mining
- Reti di Petri: analisi, modellistica e controllo - Alessandro De Luca - Università Sapienza di Roma
- Rabobank: A Process Mining Case Study: BPI Challenge 2014 Report - Queensland University of Technology, Australia

# Definizioni varie
Il punto di partenza per qualsiasi tecnica di process mining è sempre un event log (di seguito
denominato semplicemente log). Tutte le tecniche di process mining assumono che sia possibile
registrare eventi sequenzialmente in modo che ciascuno di questi si riferisca ad una determinata
attività (ciò e ad un passo ben deﬁnito di un processo) e sia associato ad una particolare istanza
di processo. Un’istanza di processo, o case, è una singola esecuzione del processo. Per esempio
si consideri il processo di gestione di prestiti elargiti da un istituto di credito: ogni esecuzione
del processo è intesa a gestire una richiesta di prestito. I log possono contenere anche ulteriori
informazioni circa gli eventi. Di fatto, quando possibile le tecniche di process mining usano informazioni supplementari come le risorse (persone e dispositivi) che eseguono o che danno inizio ad
un’attività, i timestamp o altri dati associati ad un evento (come la dimensione di un ordine).

“Costruiamo un event log a partire da dati finanziari/operativi generati dal workflow di gestione del portafoglio. Su questo log applichiamo una logica di process discovery, con riferimento teorico all’algoritmo alpha, per ottenere un modello del processo interpretabile anche come rete di Petri. Disco viene usato come strumento di analisi visuale dei casi, delle varianti e delle performance.”

Per fare process mining decidere:
- qual è il processo
- che cosa è un case
- quali sono le attività
- qual è il timestamp
- quali attributi aggiuntivi vuoi conservare

# Limite dei progetti “base” (quelli che abbiamo visto)
Quello che stavi impostando finora è:
Event log (da Python)
Discovery (Alpha algorithm)
Output → Petri net / Disco
- modello spesso semplice o poco realistico
- non cattura:
- risorse
- costi
- performance
- poco “finance-oriented” reale

# PROGETTO 5 — Object-Centric Process Mining (VERY ADVANCED)

Esempio finance:
Evento:
"execute trade"
Oggetti:
cliente
asset
ordine
Analisi:
interazioni tra oggetti
flussi complessi

# PROGETTO 6 — Predictive Process Mining (TOP assoluto)

Domande:
questo trade fallirà?
quanto tempo manca alla chiusura?
ci sarà una deviazione?
In finance:
rischio operativo
execution risk
latency

- Vorrei approfondire questi due come progetti. Sono sinceramente interessanti entrambi, tuttavia, diciamo che nel mondo della finanza lavorare su dati storici è sicuramente importante, ma lavorare in predizione è necessario. Questo mette in buona luce il progetto 6, anche se come l'hai descritto non mi convince, dal punto di vista tipo di quello che viene considerato, quindi rischio operativo, rischio di esecuzione, latency ecc... Non è possibile intrecciare i processi fatti nell'immediato passato, per ottenere una previsione di "andamento" nell'immediato futuro? Questo può essere interessante per l'intra-day che assorbe male le informazioni e che consente di "seguire le correnti" degli ordini di giornata. Magari a mercato chiuso si perde questo vantaggio competitivo rispetto al mercato, che assorbe le informazioni, le catalizza e non consente di prendere gli arbitraggi. Approfondiamoli entrambi, ti ho dato qualche spunto io. 
- Vorrei in particolare "analizzare il sentiment" di società che hanno un dual listing o un triple listing, cioè il fatto che una società sia quotata su 2/3 piazze valori differenti ti consente di carpire al meglio se ci sono trend positivi/negativi/in conflitto tra loro, ad esempio se quotata su Londra e sta aumentando, quotata su amsterdam e sta aumentando compro velocemente, poi se a NY scende vendo subito per evitare perdite e così via, un movimento che sia veloce ed "automatico", cioè il processo analizza l'andamento, tipo su/giù e ci sono dei momenti, in quanto ci sono dei momenti della giornata che per diverse socetà dualistate o triplelistate i mercati sono contemporaneamente aperti e questo può portare un gran guadagno, tipo le azioni di Unilever PLC sono quotate a Londra, Amsterdam e NY, Londra apre alle 8 e chiude alle 16:30, amsterdam dalle 9 alle 17:30 e NY 15:30-22.:00, chiaramente ci sono delle fasce orario interessanti che consentono questo tipo di attività, ad esempio dalle 15:30 alle 16:30 ci sono tre piazze aperte contemporaneamente sulle stesso titolo. mentre in altre fasce orarie due.
- Per prima cosa devi essere sicuro che i tre strumenti rappresentino davvero lo stesso sottostante economico. Nel caso Unilever, la base ufficiale è chiara: Unilever PLC è quotata a Londra e ad Amsterdam con lo stesso ISIN GB00BVZK7T90, e a New York è trattata come ADR; inoltre Unilever indica che ogni ADR rappresenta 1 azione ordinaria PLC. Quindi, per Unilever, Londra e Amsterdam sono direttamente confrontabili come stessa azione, e New York è confrontabile dopo aver tenuto conto del fatto che è un ADR 1:1 e della valuta USD.Questa è la regola generale che devi usare nel progetto: stesso issuer; stesso ISIN se possibile; se c’è un ADR, devi conoscere il rapporto ADR/azione; poi porti tutto nella stessa valuta e nella stessa unità economica.



# I titoli da vedere sono Unilever, Shell e HSBC (tutti e tre tri-listed)

- Ho fatto un primo check per Unilever e mandando analysis_ul.py si ottiene un sunto di 5.27 milioni di eventi per MBP (order book) con uno spread medio di circa 0.024, mentre per trades si ottengono 297 mila trades con prezzi compresi tra 54 e 66 circa. Ricordiamo che questi sono parameteri mensili (quindi in un solo mese, senza orderbook completo ho questi parametri)
- Prossimo step è trasformare il mercato che abbiamo definito in processo.
- Definiamo il nuovo documento quindi che porti dai dati agli eventi del processo, quindi si definisce event_log_builder.py e si ottiene il relativo csv (dentro processed)
- event_log.csv è pronto per Disco ed effettivamente funziona. Vediamo poi come analizzarlo
- quando il prezzo NON è allineato tra mercati, ma questo succede perché esiste un processo sottostante: order book changes → spread cambia → trade arriva → reazione → nuovo
- "capire il processo che porta alla differenza di prezzo", oltre che trovarle, questo potrebbe condurre a prevedere l'inefficienza, non solo a reagire. 
- Il fatto che su Disco il tempo di reazione per determinati processi sia più lento/veloce è normale, ad esempio bid_change → ask_change = velocissimo, perché il market maker è velocissimo, mentre trade → spread_widen = lento, perché il mercato "assorbe" il trade prima di reagire. 
- 