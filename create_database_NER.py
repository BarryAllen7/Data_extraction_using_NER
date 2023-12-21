import os
import pandas as pd
import re
import spacy
from tqdm import tqdm
import argparse
import json
from datetime import datetime

def lemmatize_city(city_name, nlp):
    doc = nlp(city_name)
    lemmatized_city = " ".join([token.lemma_ for token in doc])
    return lemmatized_city

def get_region_for_city(city, data, nlp):
    lemmatized_city = lemmatize_city(city, nlp)
    region_for_city = None

    for region, cities in data.items():
        for city_in_data in cities:
            lemmatized_city_in_data = lemmatize_city(city_in_data, nlp)
            if lemmatized_city == lemmatized_city_in_data:
                region_for_city = region
                break
        if region_for_city:
            break

    return region_for_city

def process_files(root_folder, output_excel, regions_data, regions_code_data):
    nlp = spacy.load('uk_core_news_trf')
    df_list = []
    for object_name in os.listdir(root_folder):
        object_path = os.path.join(root_folder, object_name)
        if os.path.isdir(object_path):
            for txt_file in tqdm(os.listdir(object_path), desc=object_name, position=0, leave=True):
                file_path = os.path.join(object_path, txt_file)
                with open(file_path, 'r', encoding='utf-8') as file:
                    text = file.read()

                date_match = re.search(r'\b\d{4}-\d{2}-\d{2}\b', text)
                if date_match:
                    message_link = text.split('\n')[-2].split(': ')[1]
                    text_from_file = text.split(f'Message Link: {message_link}')[0]
                    doc = nlp(text_from_file)
                    ner_results = [ent.text for ent in doc.ents]
                    cities_in_text = []
                    for result in ner_results:
                        cities_in_text.extend(result.split())

                    region_for_city = None
                    for city_in_text in cities_in_text:
                        region_for_city = get_region_for_city(city_in_text, regions_data, nlp)
                        if region_for_city:
                            break

                    region_code_for_city = None
                    if region_for_city:
                        region_code_for_city = regions_code_data.get(region_for_city)

                    data_dict = {
                        'channel_name': object_name,
                        'post_date': pd.to_datetime(date_match.group()),
                        'text_data': text_from_file,
                        'link': message_link,
                        'file_path': file_path,
                        'NER': ', '.join(ner_results),
                        'region': region_for_city,
                        'region_code': region_code_for_city,
                    }

                    df_list.append(data_dict)

    df = pd.DataFrame(df_list)
    df.to_excel(output_excel, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('output_excel', help='Output Excel filename.')
    args = parser.parse_args()

    # Load regions data
    with open('regions.json', 'r', encoding='utf-8') as file:
        regions_data = json.load(file)

    # Load regions code data
    with open('regions_code_ukr.json', 'r', encoding='utf-8') as file:
        regions_code_data = json.load(file)

    # start
    process_files(os.getcwd(), args.output_excel, regions_data, regions_code_data)
