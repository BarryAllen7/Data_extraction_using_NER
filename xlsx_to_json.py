import pandas as pd
import sys
import json

def convert_xlsx_to_json(xlsx_file, json_file):
    data = pd.read_excel(xlsx_file)

    # Convert post_date to datetime
    data['post_date'] = pd.to_datetime(data['post_date'], unit='ms')

    data_json = data.to_json(orient='records', force_ascii=False)

    # Parse the JSON string
    parsed_data = json.loads(data_json)

    with open(json_file, 'w', encoding='utf-8') as f:
        for record in parsed_data:
            json.dump(record, f, ensure_ascii=False)
            f.write('\n')

if len(sys.argv) != 3:
    print("Usage: python3 xlsx_to_json.py <xlsx_file> <json_file>")
else:
    xlsx_file = sys.argv[1]
    json_file = sys.argv[2]
    convert_xlsx_to_json(xlsx_file, json_file)
