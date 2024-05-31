import hashlib
import pandas as pd
import re
import os
import gzip
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from NwalaTextUtils.textutils import parallelGetTxtFrmURIs

def getURIHash(uri):
    return getStrHash(uri)

def getStrHash(txt):
    txt = txt.strip()
    if txt == '':
        return ''
    hash_object = hashlib.md5(txt.encode())
    return hash_object.hexdigest()

data = pd.read_csv('extracted_data.csv')

def extract_links(json_obj):
    article_links = []
    for k, value in json_obj.items():
        for link_entry in value.get("links", []):
            link = link_entry["link"]
            if link != '#':
                article_links.append(link)
    return article_links

def extract_year(filename):
    pattern = r'\d{4}'
    match = re.search(pattern, filename)
    if match:
        return match.group()
    else:
        return None

def create_directory(directory_path):
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
            print(f"Directory '{directory_path}' created successfully.")
        except OSError as e:
            print(f"Error creating directory '{directory_path}': {e}")
    else:
        print(f"Directory '{directory_path}' already exists.")

def get_news_org(query):
    start_index = query.find("http")
    end_index = query.find('"', start_index)
    news_org = query[start_index:end_index]
    return news_org

def process_subdir(subdir):
    _, second_dir = os.path.split(subdir)
    if second_dir != parent_directory:
        create_directory(f'state_v1/{second_dir}')
        for file_name in os.listdir(subdir):
            year = extract_year(file_name)
            output_file = f'state_v1/{second_dir}/twitter_radio_{second_dir}_{year}.jsonl.gz'
            print(f'Processing file {file_name}')
            if file_name.endswith(".jsonl.gz"):
                file_path = os.path.join(subdir, file_name)
                with gzip.open(file_path, 'rt', encoding='utf-8') as input_jsonl_file:
                    with gzip.open(output_file, 'wt') as output_jsonl_file:
                        for line in input_jsonl_file:
                            json_obj = json.loads(line)
                            links = json_obj["links"]
                            news_org = get_news_org(json_obj["query"])
                            desired_rows = data.loc[
                                (data['website'] == news_org) &
                                (data['state-code'] == second_dir) &
                                (data['media-type'] == 'radio')]
                            if not desired_rows.empty:
                                location = {
                                    "state": desired_rows['state'].values[0],
                                    "city": desired_rows['city'].values[0],
                                    "longitude": desired_rows['city-county-long'].values[0],
                                    "latitude": desired_rows['city-county-lat'].values[0],
                                }
                                for link in links:
                                    url = link.strip()
                                    try:
                                        doc_lst = parallelGetTxtFrmURIs([url], cleanHTML=False, addResponseHeader=True)
                                    except requests.exceptions.ConnectionError:
                                        print(f"Connection error occurred while accessing {url}. Skipping.")
                                        doc_lst = None
                                    except Exception as e:
                                        print(f"An error occurred: {e}")
                                        doc_lst = None

                                    if doc_lst != None and len(doc_lst[0]['info']) != 0 and len(
                                            doc_lst[0]['info']['response_history']) > 1:
                                        expanded_url = doc_lst[0]['info']['response_history'][-1]['url']
                                    else:
                                        expanded_url = None

                                    if doc_lst != None and len(doc_lst[0]['info']) > 0:
                                        title = doc_lst[0]['title']
                                    else:
                                        title = None

                                    if doc_lst != None and len(doc_lst[0]['info']) != 0 and \
                                            doc_lst[0]['info']['response_history'][-1]['status_code'] == 200:

                                        if year:
                                            html_dir = f'HTML/{second_dir}/{year}'
                                            create_directory(html_dir)
                                            html_filename = f'{html_dir}/{getURIHash(url)}.html.gz'
                                            with gzip.open(html_filename, 'wt', encoding='utf-8') as html_file:
                                                html_file.write(doc_lst[0]['text'])
                                    else:
                                        html_filename = None

                                    if doc_lst != None and len(doc_lst[0]['info']) != 0:
                                        response_code = doc_lst[0]['info']['response_history'][-1]['status_code']
                                    else:
                                        response_code = None

                                    new_json_obj = {
                                        "link": url,
                                        "publication_date": None,
                                        "media_name": desired_rows['media-name'].values[0],
                                        "media_type": desired_rows['media-type'].values[0],
                                        "location": location,
                                        "media_metadata": json.loads(desired_rows['media-metadata'].values[0]),
                                        "source": "Twitter",
                                        "source_metadata": json_obj,
                                        "title": title,
                                        "response_code": response_code,
                                        "expanded_url": expanded_url,
                                        "html_filename": html_filename
                                    }
                                    output_jsonl_file.write(json.dumps(new_json_obj) + '\n')

parent_directory = "state"
subdirs = [os.path.join(parent_directory, d) for d in os.listdir(parent_directory) if os.path.isdir(os.path.join(parent_directory, d))]

def main():
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_subdir, subdir): subdir for subdir in subdirs}
        for future in as_completed(futures):
            subdir = futures[future]
            try:
                future.result()
                print(f'Successfully processed {subdir}')
            except Exception as e:
                print(f'Error processing {subdir}: {e}')

if __name__ == "__main__":
    main()

