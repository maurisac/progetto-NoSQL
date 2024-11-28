from py2neo import Graph
import time
import csv
from scipy.stats import t
import numpy as np

percentages = [25]

def calculate_confidence_interval(data):
    data = np.array(data[1:])  # Ignora il primo tempo (prima esecuzione)
    avg_exec_time = np.mean(data)
    std_dev = np.std(data, ddof=1)
    n = len(data)

    # Calcola l'intervallo di confidenza al 95%
    t_value = t.ppf(0.975, df=n-1)  # Trova il valore critico t per il 95% di confidenza
    error_margin = t_value * (std_dev / np.sqrt(n))

    confidence_interval = (avg_exec_time - error_margin, avg_exec_time + error_margin)

    return avg_exec_time, confidence_interval

queries = [
    """
    MATCH (u:Users)-[:MADE]->(t:Transactions)-[:RECEIVED_BY]->(b:Banks)
    WHERE t.amount > 10000
    RETURN u.name AS User, b.name AS Bank, t.amount AS Amount, t.timestamp AS Time
    """,
    """
    MATCH (u:Users)-[:MADE]->(t:Transactions)
    WITH u, COUNT(t) AS numTransazioni
    WHERE numTransazioni > 50
    RETURN u.name AS User, numTransazioni AS Transazioni
    """,
    """
    MATCH (u:Users)-[:MADE]->(t:Transactions)-[:RECEIVED_BY]->(b:Banks)
    WHERE b.country IN ["CountryA", "CountryB", "CountryC"]
    RETURN u.name AS User, b.name AS Bank, t.amount AS Amount, t.timestamp AS Time
    """,
    """
    MATCH path = (u1:Users)-[:MADE]->(t1:Transactions)-[:RECEIVED_BY]->(b1:Banks)-[:RECEIVED_BY]->(t2:Transactions)<-[:MADE]-(u2:Users)-[:MADE]->(t3:Transactions)-[:RECEIVED_BY]->(b2:Banks)
    WHERE u1 = u2
    RETURN path
    """
]

first_exec_times = []
avg_exec_times = []

for percentage in percentages:
    print(f"Dimensioni dataset: {percentage}%")

    db_name = f"dataset{percentage}"
    graph = Graph(f"bolt://localhost:7687/{db_name}", user="neo4j", password="12345678", name=db_name)

    for query_num, query in enumerate(queries):
        print(f"Query {query_num + 1}")

        exec_times_query = []

        for _ in range(31):
            start = time.time()
            result = graph.run(query).data()
            end = time.time()
            total_time = (end - start) * 1000
            exec_times_query.append(total_time)

            if _ == 0:
                first_exec_time = total_time

            print(f"Risultati query {query_num + 1}:\n{result}")

        avg_exec_time, confidence_interval = calculate_confidence_interval(exec_times_query)

        print(f"Tempo di esecuzione medio (ms): {avg_exec_time}")
        print(f"Tempo della prima esecuzione (ms): {first_exec_time}")
        print(f"Intervallo di confidenza al 95%: {confidence_interval}")

        first_exec_times.append({
            "Dataset": f'{percentage}%',
            "Query": f"Query {query_num + 1}",
            "Millisecondi": first_exec_time
        })

        avg_exec_times.append({
            "Dataset": f'{percentage}%',
            "Query": f"Query {query_num + 1}",
            "Millisecondi": avg_exec_time,
            "Media": avg_exec_time,
            "Intervallo di Confidenza (Min, Max)": f"({confidence_interval[0]}, {confidence_interval[1]})"
        })

        print("-" * 40)



# Salva i risultati della prima esecuzione
csv_file_first = "G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/results/query_result_1stexec_Neo4j.csv"
with open(csv_file_first, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['Dataset', 'Query', 'Millisecondi']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for data in first_exec_times:
        writer.writerow(data)

# Salva i risultati delle 30 esecuzioni medie
csv_file_avg = "G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/results/query_result_avgexec_Neo4j.csv"
with open(csv_file_avg, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['Dataset', 'Query', 'Millisecondi', 'Media', 'Intervallo di Confidenza (Min, Max)']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for data in avg_exec_times:
        writer.writerow(data)