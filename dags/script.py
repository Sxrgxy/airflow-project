"""
python3 script.py <YYYY-mm-dd>
Ex.
python3 script.py 2024-09-10
"""

import csv
from collections import defaultdict
import os
from datetime import datetime, timedelta
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__)) 
    data_dir = os.path.join(base_dir, 'data')
    input_dir = os.path.join(data_dir, 'input')
    output_tmp_dir = os.path.join(data_dir, 'output_tmp')
    output_dir = os.path.join(data_dir, 'output')

    os.makedirs(output_tmp_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    dt = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    start_date = dt - timedelta(days=7)
    dates = [start_date + timedelta(n) for n in range(7)]

    existing_files = set(os.listdir(output_tmp_dir))
    expected_filenames = {date.strftime('%Y-%m-%d') + '.csv' for date in dates}
    missing_files = expected_filenames - existing_files

    header = None
    
    for filename in missing_files:
        input_filepath = os.path.join(input_dir, filename)
        with open(input_filepath, mode='r', newline='', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            aggregation = defaultdict(lambda: defaultdict(int))
            for row in csv_reader:
                email = row[0].strip()
                action = row[1].strip()
                aggregation[email][action] += 1

        unique_actions = sorted({action for actions in aggregation.values() for action in actions.keys()}) 

        output_tmp_filepath = os.path.join(output_tmp_dir, filename)

        with open(output_tmp_filepath, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            header = ['email'] + [action.lower() + '_count' for action in unique_actions]
            writer.writerow(header)
            for email in sorted(aggregation.keys()):
                row = [email] + [aggregation[email].get(action, 0) for action in unique_actions]
                writer.writerow(row)

    data = list()

    for filename in expected_filenames:
        output_tmp_filepath = os.path.join(output_tmp_dir, filename)

        with open(output_tmp_filepath, mode='r', newline='', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)
            data.extend(list(csv_reader))
            
    email_sums = defaultdict(lambda: [0, 0, 0, 0])

    for entry in data:
        email = entry[0]
        values = list(map(int, entry[1:]))
        email_sums[email] = [sum(x) for x in zip(email_sums[email], values)]

    if header is None:
        header = ['email', 'create_count', 'delete_count', 'read_count', 'update_count']
        logging.info("Header was None. Using default header.")
    
    output_filename = dt.strftime('%Y-%m-%d') + '.csv'
    output_filepath = os.path.join(output_dir, output_filename)

    with open(output_filepath, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)
        
        for email, totals in email_sums.items():
            writer.writerow([email] + totals)