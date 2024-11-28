import time
import csv
import requests
import numpy as np
from scipy.stats import t

percentages = [25, 50, 75, 100]

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
    for $t in //Transaction[number(amount) > 10000]
    let $u := //User[card_id = $t/sender_card_id]
    let $b := //Bank[bank_id = $t/receiver_bank_id]
    return <result>
      <User>{data($u/name)}</User>
      <Bank>{data($b/name)}</Bank>
      <Amount>{data($t/amount)}</Amount>
      <Time>{data($t/timestamp)}</Time>
    </result>
    """,
    """
    for $u in //User
    let $transactions := //Transaction[sender_card_id = $u/card_id]
    let $numTransazioni := count($transactions)
    where $numTransazioni > 50
    return <result>
      <User>{data($u/name)}</User>
      <Transazioni>{$numTransazioni}</Transazioni>
    </result>
    """,
    """
    for $t in //Transaction
    let $b := //Bank[bank_id = $t/receiver_bank_id and (country = 'CountryA' or country = 'CountryB' or country = 'CountryC')]
    where $b
    let $u := //User[card_id = $t/sender_card_id]
    return <result>
      <User>{data($u/name)}</User>
      <Bank>{data($b/name)}</Bank>
      <Amount>{data($t/amount)}</Amount>
      <Time>{data($t/timestamp)}</Time>
    </result>
    """,
    """
    for $u1 in //User
    let $t1 := //Transaction[sender_card_id = $u1/card_id]
    let $b1 := //Bank[bank_id = $t1/receiver_bank_id]
    let $t2 := //Transaction[receiver_bank_id = $b1/bank_id]
    let $u2 := //User[card_id = $t2/sender_card_id]
    let $t3 := //Transaction[sender_card_id = $u2/card_id and receiver_bank_id = $b1/bank_id]
    where $u1 = $u2
    return <path>
      <User1>{data($u1/name)}</User1>
      <Transaction1>{data($t1/amount)}</Transaction1>
      <Bank1>{data($b1/name)}</Bank1>
      <Transaction2>{data($t2/amount)}</Transaction2>
      <User2>{data($u2/name)}</User2>
      <Transaction3>{data($t3/amount)}</Transaction3>
      <Bank2>{data($b1/name)}</Bank2>
    </path>
    """
]

first_exec_times = []
avg_exec_times = []

for percentage in percentages:
    print(f"Dimensioni dataset: {percentage}%")

    db_name = f"dataset_{percentage}"

    for query_num, query in enumerate(queries):
        print(f"Query {query_num + 1}")

        exec_times_query = []

        for _ in range(31):
            start = time.time()
            response = requests.post(
                f"http://localhost:8080/rest/{db_name}",
                data=query,
                headers={'Content-Type': 'application/query+xml'},
                auth=('admin', '0')
            )
            end = time.time()
            total_time = (end - start) * 1000
            exec_times_query.append(total_time)

            if _ == 0:
                first_exec_time = total_time

            result = response.text
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
csv_file_first = "G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/results/query_result_1stexec_BaseX.csv"
with open(csv_file_first, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['Dataset', 'Query', 'Millisecondi']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for data in first_exec_times:
        writer.writerow(data)

# Salva i risultati delle 30 esecuzioni medie
csv_file_avg = "G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/results/query_result_avgexec_BaseX.csv"
with open(csv_file_avg, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['Dataset', 'Query', 'Millisecondi', 'Media', 'Intervallo di Confidenza (Min, Max)']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for data in avg_exec_times:
        writer.writerow(data)
