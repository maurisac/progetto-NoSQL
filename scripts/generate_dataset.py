import csv
import random
from faker import Faker
from datetime import datetime, timedelta

absolute_path = "G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/"

fake = Faker()

# Parametri base
NUM_USERS = 10000
NUM_BANKS = 500
NUM_TRANSACTIONS = 30000
HIGH_RISK_COUNTRIES = ["Afghanistan", "Filippine", "Marocco"]

percentages = [100, 75, 50, 25]

for percentage in percentages:
    # Cambia il numero di record per ogni entità inbase alla percentuale
    num_users = int(NUM_USERS * (percentage / 100))
    num_banks = int(NUM_BANKS * (percentage / 100))
    num_transactions = int(NUM_TRANSACTIONS * (percentage / 100))

    # Generazione banche
    banks = []
    for i in range(1, num_banks + 1):
        country = random.choice(HIGH_RISK_COUNTRIES + ["Italia", "Canada", "Australia"])
        banks.append([i, fake.company(), country])

    # Salvataggio delle banche in CSV
    with open(fr'{absolute_path}dataset/{percentage}%/dataset_banks{percentage}%.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['bank_id', 'name', 'country'])
        writer.writerows(banks)

    # Generazione utenti
    users = []
    for i in range(1, num_users + 1):
        bank_id = random.randint(1, num_banks)
        card_id = fake.unique.credit_card_number()
        users.append([i, fake.name(), fake.email(), fake.date_of_birth(minimum_age=18, maximum_age=85), card_id, bank_id])

    # Salvataggio degli utenti in CSV
    with open(fr'{absolute_path}dataset/{percentage}%/dataset_users{percentage}%.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['user_id', 'name', 'email', 'birth_date', 'card_id', 'bank_id'])
        writer.writerows(users)

    # Generazione transazioni
    transactions = []
    for i in range(1, num_transactions + 1):
        sender = random.choice(users)
        sender_card_id = sender[4]
        receiver_bank_id = random.randint(1, num_banks)
        amount = round(random.uniform(10, 10000), 2)
        if random.random() < 0.05:  # 5% di chance di avere una transazione anomala
            amount *= 10  # Decuplico l'importo della transazione per renderla sospetta
        timestamp = (datetime.now() - timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d %H:%M:%S')
        transactions.append([i, sender_card_id, receiver_bank_id, amount, timestamp])

    # Salvataggio delle transazioni in CSV
    with open(fr'{absolute_path}dataset/{percentage}%/dataset_transactions{percentage}%.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['transaction_id', 'sender_card_id', 'receiver_bank_id', 'amount', 'timestamp'])
        writer.writerows(transactions)

    print(f"Files for {percentage}% dataset generated successfully.")
