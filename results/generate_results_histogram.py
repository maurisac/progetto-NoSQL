import pandas as pd
import matplotlib.pyplot as plt
import re

# Carica i dati dai file CSV
data_first_execution_basex = pd.read_csv("G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/results/query_result_1stexec_BaseX.csv")
data_avg_execution_basex = pd.read_csv("G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/results/query_result_avgexec_BaseX.csv")
data_first_execution_neo4j = pd.read_csv("G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/results/query_result_1stexec_Neo4j.csv")
data_avg_execution_neo4j = pd.read_csv("G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/results/query_result_avgexec_Neo4j.csv")

# Definisci i colori per i grafici
color_basex = 'blue'
color_neo4j = 'green'

# Definisci le dimensioni del dataset e le query
dataset_sizes = ['25%', '50%', '75%', '100%']
queries = ['Query 1', 'Query 2', 'Query 3', 'Query 4', 'Query 5']

def extract_confidence_values(confidence_interval_str):
    matches = re.findall(r'\d+\.\d+', confidence_interval_str)
    return float(matches[0]), float(matches[1])

# Per ogni query, crea gli istogrammi
for query in queries:
    # Filtra i dati per la query corrente
    data_first_execution_basex_query = data_first_execution_basex[data_first_execution_basex['Query'] == query]
    data_avg_execution_basex_query = data_avg_execution_basex[data_avg_execution_basex['Query'] == query]
    data_first_execution_neo4j_query = data_first_execution_neo4j[data_first_execution_neo4j['Query'] == query]
    data_avg_execution_neo4j_query = data_avg_execution_neo4j[data_avg_execution_neo4j['Query'] == query]
    
    # Crea il primo istogramma con i tempi della prima esecuzione
    plt.figure(figsize=(10, 6))
    for size in dataset_sizes:
        values_basex = data_first_execution_basex_query[data_first_execution_basex_query['Dataset'] == size]['Millisecondi']
        values_neo4j = data_first_execution_neo4j_query[data_first_execution_neo4j_query['Dataset'] == size]['Millisecondi']
        
        plt.bar([f"{size} (BaseX)", f"{size} (Neo4j)"], [values_basex.values[0], values_neo4j.values[0]], color=[color_basex, color_neo4j])
        
    plt.xlabel('Dimensione del Dataset')
    plt.ylabel('Tempo di esecuzione (ms)')
    plt.title(f'Istogramma - Tempo della Prima Esecuzione per {query}')
    plt.legend()
    plt.tight_layout()
    plt.show()

    # Crea il secondo istogramma con le medie dei tempi
    plt.figure(figsize=(10, 6))
    for size in dataset_sizes:
        values_basex = data_avg_execution_basex_query[data_avg_execution_basex_query['Dataset'] == size]['Millisecondi']
        values_neo4j = data_avg_execution_neo4j_query[data_avg_execution_neo4j_query['Dataset'] == size]['Millisecondi']
        
        # Estrae intervalli di confidenza
        confidence_intervals_basex = data_avg_execution_basex_query[data_avg_execution_basex_query['Dataset'] == size]['Intervallo di Confidenza (Min, Max)']
        confidence_intervals_neo4j = data_avg_execution_neo4j_query[data_avg_execution_neo4j_query['Dataset'] == size]['Intervallo di Confidenza (Min, Max)']
        conf_intervals_basex = [extract_confidence_values(conf_str) for conf_str in confidence_intervals_basex]
        conf_intervals_neo4j = [extract_confidence_values(conf_str) for conf_str in confidence_intervals_neo4j]
        
        # Estrae valori minimi e massimi dagli intervalli di confidenza
        conf_basex_min = [conf[0] for conf in conf_intervals_basex]
        conf_basex_max = [conf[1] for conf in conf_intervals_basex]
        conf_neo4j_min = [conf[0] for conf in conf_intervals_neo4j]
        conf_neo4j_max = [conf[1] for conf in conf_intervals_neo4j]
        
        # Calcola la differenza tra il valore medio e gli estremi dell'intervallo di confidenza
        basex_yerr = [[values_basex.values[0] - conf_basex_min[0]], [conf_basex_max[0] - values_basex.values[0]]]
        neo4j_yerr = [[values_neo4j.values[0] - conf_neo4j_min[0]], [conf_neo4j_max[0] - values_neo4j.values[0]]]
        
        # Rappresenta l'intervallo di confidenza per BaseX e Neo4j
        plt.bar(f"{size} (BaseX)", values_basex.values[0], yerr=basex_yerr, capsize=5, color=color_basex, label='BaseX')
        plt.bar(f"{size} (Neo4j)", values_neo4j.values[0], yerr=neo4j_yerr, capsize=5, color=color_neo4j, label='Neo4j')
        
    plt.xlabel('Dimensione del Dataset')
    plt.ylabel('Tempo di esecuzione medio (ms)')
    plt.title(f'Istogramma - Tempo Medio di Esecuzione per {query}')
    plt.legend()
    plt.tight_layout()
    plt.show()