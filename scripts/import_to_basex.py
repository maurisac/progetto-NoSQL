import os
import csv
import xml.etree.ElementTree as ET
import requests

def convert_row_to_xml(row, root_element, id_field=None):
    element = ET.Element(root_element)
    if id_field:
        element.set('ID', row[id_field])
    for col, val in row.items():
        child = ET.SubElement(element, col)
        child.text = str(val)
    return element

def upload_xml_to_basex(xml_content, db_name):
    url = f"http://localhost:8080/rest/{db_name}"
    headers = {'Content-Type': 'application/xml'}
    response = requests.put(url, data=xml_content, headers=headers, auth=('admin', '0'))
    if response.status_code in [200, 201]:
        print(f"XML file uploaded successfully to {db_name}.")
    else:
        print(f"Failed to upload XML file to {db_name}: {response.status_code} {response.text}")

def create_and_upload_datasets(root_directory):
    percentages = ['25%', '50%', '75%', '100%']
    data_types = ['users', 'banks', 'transactions']
    xml_root_elements = {
        'users': 'User',
        'banks': 'Bank',
        'transactions': 'Transaction'
    }
    id_fields = {
        'users': 'user_id',
        'banks': 'bank_id',
        'transactions': 'transaction_id'
    }

    for percentage in percentages:
        root = ET.Element('Data')

        for data_type in data_types:
            csv_file = f'dataset_{data_type}{percentage}.csv'
            xml_root_element = xml_root_elements[data_type]
            id_field = id_fields[data_type]
            csv_path = os.path.join(root_directory, percentage, csv_file)

            if os.path.exists(csv_path):
                with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        xml_data = convert_row_to_xml(row, xml_root_element, id_field)
                        root.append(xml_data)

                        # Aggiungi relazioni qui
                        if data_type == 'transactions':
                            user_id = row['sender_card_id']
                            bank_id = row['receiver_bank_id']
                            transaction_id = row['transaction_id']

                            transaction_element = root.find(f".//Transaction[@ID='{transaction_id}']")
                            if transaction_element is not None:
                                transaction_element.set('user_ref', user_id)
                                transaction_element.set('bank_ref', bank_id)

            else:
                print(f"File not found: {csv_path}")

        # Aggiungi la dichiarazione XML
        xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>'
        xml_content = xml_declaration + ET.tostring(root, encoding='unicode', method='xml')

        # Carica il contenuto XML su BaseX
        upload_xml_to_basex(xml_content, f"dataset_{percentage}")

create_and_upload_datasets("G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/dataset")