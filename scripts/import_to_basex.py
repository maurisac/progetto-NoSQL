import csv
import xml.etree.ElementTree as ET
import requests
import os

def convert_row_to_xml(row, root_element):
    root = ET.Element(root_element)
    for col, val in row.items():
        child = ET.SubElement(root, col)
        child.text = str(val)
    return ET.tostring(root, encoding='utf-8', method='xml')

def upload_xml_to_basex(xml_content, db_name):
    url = f"http://localhost:8080/rest/{db_name}"
    headers = {'Content-Type': 'application/xml'}
    response = requests.put(url, data=xml_content, headers=headers, auth=('admin', '0'))  
    if response.status_code == 200 or response.status_code == 201:
        print(f"XML file uploaded successfully to {db_name}.")
    else:
        print(f"Failed to upload XML file to {db_name}: {response.status_code} {response.text}")

def create_and_upload_datasets(root_directory):
    percentages = ['100%', '75%', '50%', '25%']
    data_types = ['users', 'banks', 'transactions']
    xml_root_elements = {
        'users': 'User',
        'banks': 'Bank',
        'transactions': 'Transaction'
    }

    for percentage in percentages:
        root = ET.Element('Data')
        db_name = f'dataset_{percentage}'

        for data_type in data_types:
            csv_file = f'dataset_{data_type}{percentage}.csv'
            xml_root_element = xml_root_elements[data_type]
            csv_path = os.path.join(root_directory, percentage, csv_file)

            if os.path.exists(csv_path):
                with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)

                    for row in reader:
                        xml_data = convert_row_to_xml(row, xml_root_element)
                        root.append(ET.fromstring(xml_data))
            else:
                print(f"File not found: {csv_path}")

        tree = ET.ElementTree(root)
        xml_content = ET.tostring(tree.getroot(), encoding='utf-8', method='xml')
        upload_xml_to_basex(xml_content, db_name)

# Example usage
root_directory = "G:/ROBA DI MAURIZIO/UNIVERSITA'/basi 2/progetto-DB2-Saccà/dataset"
create_and_upload_datasets(root_directory)
