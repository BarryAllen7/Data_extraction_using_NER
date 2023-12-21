import os
import json
from telethon import TelegramClient, sync
import logging
import spacy

# Load model for Ukrainian language and analyse messages from *.txt files using NLP
nlp = spacy.load("uk_core_news_trf")
def extract_information(file_path, message_time, message_link):
    with open(file_path, "r", encoding="utf-8") as file:
        text = file.read()
    doc = nlp(text)
    # Extract entities using spaCy's NER
    entities = [(ent.text, ent.label_) for ent in doc.ents]
    return {"message_time": message_time, "message_link": message_link, "extracted_info": entities}

# Colelct messages from Telegram channels
def collect_data(api_id, api_hash, channels, directory_path):
    logging.basicConfig(level=logging.DEBUG)
    client = TelegramClient('client', api_id, api_hash)
    async def save_messages(channel):
        async for message in client.iter_messages(channel):
            if message.text:
                folder_name = os.path.join(directory_path, f'{channel[1:]}')
                os.makedirs(folder_name, exist_ok=True)
                file_path = os.path.join(folder_name, f'{channel[1:]}_{message.id}.txt')
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(message.text)
                    message_link = f"https://t.me/{channel[1:]}/{message.id}"
                    file.write(f"\n\nMessage Link: {message_link}")
                    file.write(f"\nPublication Date: {message.date}")

    with client:
        for channel in channels:
            client.loop.run_until_complete(save_messages(channel))
            print(f"Messages from {channel} collected.")

# Iterate all text files in the directory
def process_files(directory_path, channels):
    for channel in channels:
        results_for_channel = []

        channel_directory_path = os.path.join(directory_path, channel[1:])
        for filename in os.listdir(channel_directory_path):
            if filename.endswith(".txt"):
                file_path = os.path.join(channel_directory_path, filename)
                message_time = None
                message_link = None

                # Extract the message link and publication date from the text file
                with open(file_path, "r", encoding="utf-8") as file:
                    lines = file.readlines()
                    if lines:
                        last_line = lines[-1]
                        if last_line.startswith("Message Link:"):
                            message_link = last_line.split(":")[1].strip()
                        if last_line.startswith("Publication Date:"):
                            message_time = last_line.split(":")[1].strip()

                print("\nExtracting information from:", file_path)
                extracted_info = extract_information(file_path, message_time, message_link)
                print("Extracted Information:", extracted_info)
                results_for_channel.append({
                    "file_name": filename,
                    "extracted_info": extracted_info
                })

        # Save as JSON
        output_path = f"{channel[1:]}_result.json"
        with open(output_path, "w", encoding="utf-8") as json_file:
            json.dump(results_for_channel, json_file, ensure_ascii=False, indent=4)
        print(f"\nData from {channel} saved to:", output_path)

if __name__ == "__main__":
    # Telegram API
    api_id = '' #{add api_id}
    api_hash = '' #{add api_hash}

    #define channels 
    channels_to_collect = ['@rada_en', '@dsszzi_official', '@dsns_telegram', '@operativnoZSU']
    
    #define path to doanload files
    downloaded_data_path = "/root/NER/cyber_army/"
    #start collecting data
    collect_data(api_id, api_hash, channels_to_collect, downloaded_data_path)
    #start analysing files
    process_files(downloaded_data_path, channels_to_collect)
