import os
import pandas as pd
from py2neo import Graph, Node, Relationship

# Configura la cartella in cui si trovano i dataset
script_directory = os.path.dirname(os.path.abspath(__file__))

# Connessione ai diversi database Neo4j per ciascuna percentuale di dataset
graphs = {
    100: Graph("bolt://localhost:7687", user="neo4j", password="password", name="dataset100"),
    75: Graph("bolt://localhost:7687", user="neo4j", password="password", name="dataset75"),
    50: Graph("bolt://localhost:7687", user="neo4j", password="password", name="dataset50"),
    25: Graph("bolt://localhost:7687", user="neo4j", password="password", name="dataset25")
}

# Categorie di dati per iterare
data_categories = ['users', 'banks', 'transactions']

# Funzione per creare indici e vincoli (se non esistono)
def setup_database(graph):
    graph.run("CREATE CONSTRAINT IF NOT EXISTS ON (u:User) ASSERT u.user_id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS ON (b:Bank) ASSERT b.bank_id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS ON (t:Transaction) ASSERT t.transaction_id IS UNIQUE;")

# Importazione dei dati
for percentage, graph in graphs.items():
    setup_database(graph)  # Configura il database se necessario
    for category in data_categories:
        csv_path = os.path.join(script_directory, f'{percentage}%', f'dataset_{category}{percentage}%.csv')
        data = pd.read_csv(csv_path, encoding='utf-8')

        for index, row in data.iterrows():
            node = Node(category.capitalize(), **row.to_dict())
            graph.merge(node, category.capitalize(), 'id')

            # Se è una transazione, potresti voler collegarla a utenti o banche
            if category == 'transactions':
                # Supponendo che ci sia 'user_id' e 'bank_id' nelle transazioni
                user_node = graph.nodes.match("User", id=row['user_id']).first()
                bank_node = graph.nodes.match("Bank", id=row['bank_id']).first()

                if user_node and bank_node:
                    user_rel = Relationship(user_node, "PERFORMED", node)
                    bank_rel = Relationship(node, "TO_BANK", bank_node)
                    graph.create(user_rel)
                    graph.create(bank_rel)

        print(f"Dati per {category} al {percentage}% importati con successo in Neo4j.")

print("Importazione completata per tutti i dataset.")
