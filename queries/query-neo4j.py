from py2neo import Graph
import time
import csv
from scipy.stats import t
import numpy as np

percentages = [25]#, 50, 75, 100]

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
    # Query 1: Transazioni con importo superiore a 50000
    """
    MATCH (t:Transactions) WHERE t.amount > 50000
    RETURN t
    """,

    # Query 2: Tutti gli utenti appartenenti alle banche in paesi ad alto rischio
    """
    MATCH (u:Users)-[:PERFORMED]->(t:Transactions)-[:TO_BANK]->(b:Banks)
    WHERE b.country IN ["Afghanistan", "Filippine", "Marocco"]
    RETURN u.name AS User, b.name AS Bank, t.amount AS Amount, t.timestamp AS Time
    """,

    # Query 3: Utenti che effettuano transazioni in paesi diversi
    """
    MATCH (u:Users)-[:PERFORMED]->(t:Transactions)-[:TO_BANK]->(b:Banks)
    WHERE t.amount > 5000
    WITH u, COLLECT(DISTINCT b.country) AS countries, COUNT(t) AS totalTransactions
    WHERE SIZE(countries) > 1
    RETURN u.name AS User, countries AS Countries, totalTransactions AS TotalTransactions
    """,


    # Query 4: Tante piccole transazioni da parte di più di 30 utenti che vengono eseguite nello stesso minuto
    """
    MATCH (t:Transactions)
    WITH t, split(t.timestamp, " ")[0] + " " + substring(split(t.timestamp, " ")[1], 0, 5) AS minute
    WHERE t.amount < 5000
    WITH minute, COUNT(DISTINCT t.sender_card_id) AS numUsers, COLLECT(t) AS transactions
    WHERE numUsers > 10
    RETURN minute AS Minute, numUsers AS Users, SIZE(transactions) AS TotalTransactions
    """
]

def automate_queries():
    first_exec_times = []
    avg_exec_times = []
    query_results = []

    for percentage in percentages:
        print(f"Dimensioni dataset: {percentage}%")

        db_name = f"dataset{percentage}"
        graph = Graph(f"bolt://localhost:7687", auth=("neo4j", "12345678"), name=db_name)

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

                if _ == 0:  # Salva l'output della prima esecuzione
                    query_results.append({
                        "Dataset": f'{percentage}%',
                        "Query": f"Query {query_num + 1}",
                        "Result": result
                    })

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

    # Salva l'output delle query
    csv_file_results = "G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/results/query_results_Neo4j.csv"
    with open(csv_file_results, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Dataset', 'Query', 'Result']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for data in query_results:
            writer.writerow(data)

if __name__ == "__main__":
    automate_queries()