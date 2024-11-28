import time
import csv
import numpy as np
from scipy.stats import t
from BaseXClient import BaseXClient

# Parametri di connessione per BaseX
host = "localhost"
port = 8984
username = "admin"
password = "0"

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

def run_query(session, query_id, query, query_description):
    try:
        start_time = time.time()
        # Esegue la query usando il comando XQUERY di BaseX
        command = f"XQUERY {query}"
        result = session.execute(command)
        end_time = time.time()
        time_taken = (end_time - start_time) * 1000  # Calcola il tempo impiegato in ms

        # Scrive i tempi di esecuzione nel file
        write_query_time(query_id, query_description, time_taken)

        # Ritorna il risultato della query e il tempo impiegato
        return result, time_taken
    except Exception as e:
        print(f"Error executing query {query_id}: {e}")
        return None, None

def write_query_time(query_id, query_description, time_taken):
    with open('query_times.txt', mode='a', encoding='utf-8') as f:
        f.write(f"Query ID: {query_id}, Query Description: {query_description}, Time Taken: {time_taken:.4f} ms\n")

def automate_queries():
    percentages = [25]#, 50, 75, 100]
    queries = [
        # Query 1: Transazioni con importo superiore a 50000
        (1, """
        for $t in //Transaction[number(amount) > 50000]
        let $u := //User[card_id = $t/sender_card_id]
        let $b := //Bank[bank_id = $t/receiver_bank_id]
        return <result>
          <User>{data($u/name)}</User>
          <Bank>{data($b/name)}</Bank>
          <Amount>{data($t/amount)}</Amount>
          <Time>{data($t/timestamp)}</Time>
        </result>
        """, "Transazioni con importo superiore a 50000"),

        # Query 2: Tutti gli utenti appartenenti alle banche in paesi ad alto rischio
        (2, """
        for $t in //Transaction
        let $b := //Bank[bank_id = $t/receiver_bank_id and (country = 'Afghanistan' or country = 'Filippine' or country = 'Marocco')]
        where $b
        let $u := //User[card_id = $t/sender_card_id]
        return <result>
          <User>{data($u/name)}</User>
          <Bank>{data($b/name)}</Bank>
          <Amount>{data($t/amount)}</Amount>
          <Time>{data($t/timestamp)}</Time>
        </result>
        """, "Tutti gli utenti appartenenti alle banche in paesi ad alto rischio")
        # ,

        # # Query 3: Utenti che effettuano transazioni multiple verso banche in paesi diversi e il totale è sopra i 30000
        # (3, """
        # for $u in //User
        # let $t1 := //Transaction[sender_card_id = $u/card_id]
        # let $b1 := //Bank[bank_id = $t1/receiver_bank_id]
        # for $t2 in //Transaction[sender_card_id = $u/card_id and receiver_bank_id != $t1/receiver_bank_id]
        # let $b2 := //Bank[bank_id = $t2/receiver_bank_id]
        # where number($t1/amount) + number($t2/amount) > 30000
        # return <result>
        #   <User>{data($u/name)}</User>
        #   <Bank1>{data($b1/name)}</Bank1>
        #   <Country1>{data($b1/country)}</Country1>
        #   <Amount1>{data($t1/amount)}</Amount1>
        #   <Time1>{data($t1/timestamp)}</Time1>
        #   <Bank2>{data($b2/name)}</Bank2>
        #   <Country2>{data($b2/country)}</Country2>
        #   <Amount2>{data($t2/amount)}</Amount2>
        #   <Time2>{data($t2/timestamp)}</Time2>
        # </result>
        # """, "Utenti che effettuano transazioni multiple verso banche in paesi diversi e il totale è sopra i 30000"),

        # # Query 4: Utenti che inoltrano soldi tra banche in paesi ad alto rischio
        # (4, """
        # for $t1 in //Transaction
        # let $u1 := //User[card_id = $t1/sender_card_id]
        # let $b1 := //Bank[bank_id = $t1/receiver_bank_id and (country = 'Afghanistan' or country = 'Filippine' or country = 'Marocco')]
        # where $b1
        # for $t2 in //Transaction[sender_card_id = $u1/card_id and receiver_bank_id = $b1/bank_id and number(amount) <= number($t1/amount) and $t2/timestamp > $t1/timestamp]
        # let $u2 := //User[card_id = $t2/sender_card_id]
        # let $b2 := //Bank[bank_id = $t2/receiver_bank_id and (country = 'Afghanistan' or country = 'Filippine' or country = 'Marocco')]
        # where $b2 and number($t1/amount) + number($t2/amount) >= 30000
        # return <result>
        #   <User1>{data($u1/name)}</User1>
        #   <Bank1>{data($b1/name)}</Bank1>
        #   <Amount1>{data($t1/amount)}</Amount1>
        #   <Time1>{data($t1/timestamp)}</Time1>
        #   <User2>{data($u2/name)}</User2>
        #   <Bank2>{data($b2/name)}</Bank2>
        #   <Amount2>{data($t2/amount)}</Amount2>
        #   <Time2>{data($t2/timestamp)}</Time2>
        # </result>
        # """, "Utenti che inoltrano soldi tra banche in paesi ad alto rischio")
    ]

    first_exec_times = []
    avg_exec_times = []
    query_results = []

    for percentage in percentages:
        print(f"Dimensioni dataset: {percentage}%")

        db_name = f"dataset_{percentage}%"

        # Crea una sessione BaseX
        session = BaseXClient.Session(host, port, username, password)

        try:
            # Apre il database specificato
            session.execute(f"OPEN {db_name}")

            for query_id, query, description in queries:
                print(f"Running Query {query_id}: {description}")
                exec_times_query = []

                for _ in range(31):
                    start = time.time()
                    result = session.execute(f'XQUERY {query}')
                    end = time.time()
                    total_time = (end - start) * 1000
                    exec_times_query.append(total_time)

                    if _ == 0:
                        first_exec_time = total_time

                    if _ == 0:  # Salva l'output della prima esecuzione
                        query_results.append({
                            "Dataset": f'{percentage}%',
                            "Query": f"Query {query_id}",
                            "Result": result
                        })

                    print(f"Risultati query {query_id}:\n{result}")

                avg_exec_time, confidence_interval = calculate_confidence_interval(exec_times_query)

                print(f"Tempo di esecuzione medio (ms): {avg_exec_time}")
                print(f"Tempo della prima esecuzione (ms): {first_exec_time}")
                print(f"Intervallo di confidenza al 95%: {confidence_interval}")

                first_exec_times.append({
                    "Dataset": f'{percentage}%',
                    "Query": f"Query {query_id}",
                    "Millisecondi": first_exec_time
                })

                avg_exec_times.append({
                    "Dataset": f'{percentage}%',
                    "Query": f"Query {query_id}",
                    "Millisecondi": avg_exec_time,
                    "Media": avg_exec_time,
                    "Intervallo di Confidenza (Min, Max)": f"({confidence_interval[0]}, {confidence_interval[1]})"
                })

                print("-" * 40)

            # Scrive una linea vuota nel file di output per separare le esecuzioni
            with open('query_times.txt', mode='a', encoding='utf-8') as f:
                f.write("\n\n")
        finally:
            # Assicura che la sessione BaseX sia chiusa correttamente alla fine
            session.close()

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

    # Salva l'output delle query
    csv_file_results = "G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/results/query_results_BaseX.csv"
    with open(csv_file_results, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Dataset', 'Query', 'Result']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for data in query_results:
            writer.writerow(data)

if __name__ == "__main__":
    automate_queries()