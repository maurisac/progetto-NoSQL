import os
import pandas as pd
from py2neo import Graph, Node, Relationship

# Configura la cartella in cui si trovano i dataset
absolute_path = "G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/"

# Connessione ai diversi database Neo4j per ciascuna percentuale di dataset
graphs = {
    25: Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset25"),
    50: Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset50"),
    75: Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset75"),
    100: Graph("bolt://localhost:7687", user="neo4j", password="12345678", name="dataset100")
}

# Categorie di dati per iterare
data_categories = ['users', 'banks', 'transactions']

# Funzione per creare indici e vincoli (se non esistono)
def setup_database(graph):
    graph.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:Users) REQUIRE u.user_id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS FOR (b:Banks) REQUIRE b.bank_id IS UNIQUE;")
    graph.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Transactions) REQUIRE t.transaction_id IS UNIQUE;")

# Importazione dei dati
for percentage, graph in graphs.items():
    # if percentage >= 50 : exit("Caricati i dataset al 25%")
    setup_database(graph)  # Configura il database se necessario
    for category in data_categories:
        csv_path = os.path.join(absolute_path, f'dataset/{percentage}%', f'dataset_{category}{percentage}%.csv')
        data = pd.read_csv(csv_path, encoding='utf-8')

        for index, row in data.iterrows():
            node = Node(category.capitalize(), **row.to_dict())
            graph.merge(node, category.capitalize(), f'{category[:-1]}_id')

            # Se è una transazione, potresti voler collegarla a utenti o banche
            if category == 'transactions':
            # Cerca la corrispondenza della carta con un utente
                card_node = graph.nodes.match("Users", card_id=row['sender_card_id']).first()
                bank_node = graph.nodes.match("Banks", bank_id=row['receiver_bank_id']).first()
                if card_node and bank_node:
                    user_rel = Relationship(card_node, "PERFORMED", node)
                    bank_rel = Relationship(node, "TO_BANK", bank_node)
                    graph.create(user_rel)
                    graph.create(bank_rel)

        print(f"Dati per {category} al {percentage}% importati con successo in Neo4j.")

print("Importazione completata per tutti i dataset.")
