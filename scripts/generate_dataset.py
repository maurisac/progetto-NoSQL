import csv
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Parametri base
NUM_USERS = 10000
NUM_BANKS = 500
NUM_TRANSACTIONS = 30000
HIGH_RISK_COUNTRIES = ["Afghanistan", "Filippine", "Marocco"]

# Percorsi
absolute_path = "G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/"
data_types = ['banks', 'users', 'transactions']
percentages = [100, 75, 50, 25]

# Generazione dati al 100%
full_datasets = {
    'banks': [],
    'users': [],
    'transactions': []
}

# Genera banche
for i in range(1, NUM_BANKS + 1):
    country = random.choice(HIGH_RISK_COUNTRIES + ["Italia", "Canada", "Australia"])
    full_datasets['banks'].append([i, fake.company(), country])

# Genera utenti
for i in range(1, NUM_USERS + 1):
    bank_id = random.randint(1, NUM_BANKS)
    card_id = fake.unique.credit_card_number()
    full_datasets['users'].append([i, fake.name(), fake.email(), fake.date_of_birth(minimum_age=18, maximum_age=85), card_id, bank_id])

# Genera transazioni
for i in range(1, NUM_TRANSACTIONS + 1):
    sender = random.choice(full_datasets['users'])
    sender_card_id = sender[4]
    receiver_bank_id = random.randint(1, NUM_BANKS)
    amount = round(random.uniform(10, 10000), 2)
    if random.random() < 0.05:  # 5% di chance di avere una transazione anomala
        amount *= 10  # Decuplico l'importo della transazione per renderla sospetta
    timestamp = (datetime.now() - timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d %H:%M:%S')
    full_datasets['transactions'].append([i, sender_card_id, receiver_bank_id, amount, timestamp])

# Salvataggio dei dati per ogni percentuale
for percentage in percentages:
    for data_type in data_types:
        num_records = int(len(full_datasets[data_type]) * (percentage / 100))
        csv_path = fr'{absolute_path}dataset/{percentage}%/dataset_{data_type}{percentage}%.csv'
        with open(csv_path, 'w', newline='') as file:
            writer = csv.writer(file)
            if data_type == 'banks':
                writer.writerow(['bank_id', 'name', 'country'])
            elif data_type == 'users':
                writer.writerow(['user_id', 'name', 'email', 'birth_date', 'card_id', 'bank_id'])
            elif data_type == 'transactions':
                writer.writerow(['transaction_id', 'sender_card_id', 'receiver_bank_id', 'amount', 'timestamp'])
            writer.writerows(full_datasets[data_type][:num_records])

    print(f"Files for {percentage}% dataset generated successfully.")
