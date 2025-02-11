import pandas as pd
import re
import requests
from concurrent.futures import ThreadPoolExecutor
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import random
import tldextract

# Configure logging
logging.basicConfig(filename="Clean Duplicates/clean duplicates from two files/clean_and_process_main_database/debug.log", level=logging.INFO, format="%(asctime)s - %(message)s")

# Google Sheets setup
SERVICE_ACCOUNT_FILE = 'Requirements Files/GOOGLE_CREDENTIALS_FILE.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SHEET_ID = "1o5kGS8rhHru6wsiSSiXRTsp3P1Id_5SQWt8fyXOkYK8"
SHEET_NAME = "Games"

# Helper Functions
def normalize_url(url):
    bogus_keywords = [
        "promotion", "casino", "welcome", "offers", "promo", "restricted",
        "promotions", "play", "games", "start", "go", "site", "lp",
        "goto", "records", "record", "click", "exclusive", "secure",
        "aff", "promos", "affiliatemedia", "partners", "media",
        "aff-promo", "partner", "ads", "affi", "affiliate", "affiliates", "maintenance", "test",
        "img", "mail", "private", "encrypted-tbn0", "static"
    ]
    if isinstance(url, str):
        url = url.lower().strip()
        url = re.sub(r'^(https?://)?(www\.)?', '', url)
        url = re.sub(r':\d+', '', url)
        url = url.split('/')[0].split('?')[0]
        url_parts = url.split('.')
        while url_parts and url_parts[0] in bogus_keywords:
            url_parts.pop(0)
        return '.'.join(url_parts) if len(url_parts) > 1 else ''.join(url_parts)
    return ''

def process_for_check(domain):
    if pd.isna(domain) or not isinstance(domain, str):
        return domain
    extracted = tldextract.extract(domain)
    return f"{extracted.domain}.{extracted.suffix}"

def get_incremental_key(domain):
    parts = domain.split('.')
    max_lead = -1
    max_trail = -1
    best_key = None

    def extract_numbers(part):
        lead_match = re.match(r'^(\d+)', part)
        trail_match = re.search(r'(\d+)$', part)
        lead_num = int(lead_match.group(1)) if lead_match else -1
        trail_num = int(trail_match.group(1)) if trail_match else -1
        base_part = re.sub(r'^\d+|\d+$', '', part)
        return lead_num, trail_num, base_part

    for i in range(len(parts)):
        lead, trail, base = extract_numbers(parts[i])
        key = '.'.join(parts[:i] + [base] + parts[i+1:])

        if (lead > max_lead) or (lead == max_lead and trail > max_trail):
            max_lead = lead
            max_trail = trail
            best_key = key

    return (best_key, max_lead, max_trail) if best_key else (None, -1, -1)


def resolve_redirect(url):
    try:
        response = requests.get(f"http://{url}", allow_redirects=True, timeout=5)
        return normalize_url(response.url)
    except requests.RequestException:
        return url

def resolve_redirects_in_batch(urls, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return list(executor.map(resolve_redirect, urls))

def extract_name_from_domain(domain):
    if isinstance(domain, str):
        return domain.split('.')[0]
    return ""

def fetch_google_sheet_data(sheet_id, sheet_name):
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range=sheet_name).execute()
        values = result.get('values', [])

        if not values:
            raise ValueError("No data found in the Google Sheet.")

        df = pd.DataFrame(values[1:], columns=values[0])
        return df
    except HttpError as err:
        logging.error(f"Google Sheets API error: {err}")
        raise

def select_vpn():
    vpn_options = [
        "Balkans", "United States", "Canada", "United Kingdom", "Germany", "France",
        "Australia", "New Zealand", "Japan", "South Korea", "India",
        "Brazil", "Mexico", "South Africa", "Italy", "Spain",
        "Netherlands", "Sweden", "Norway", "Denmark", "Finland",
        "Russia", "China", "Singapore", "Malaysia", "Indonesia",
        "Switzerland", "Ireland", "Austria", "Poland", "Portugal", "Sri Lanka", "TBD"
    ]
    print("\nAvailable VPNs:")
    for idx, vpn in enumerate(vpn_options, start=1):
        print(f"{idx}. {vpn}")

    while True:
        try:
            choice = int(input("\nSelect a VPN by entering the number (1-33): "))
            if 1 <= choice <= len(vpn_options):
                return vpn_options[choice - 1]
            else:
                print("Invalid choice. Please enter a number between 1 and 33.")
        except ValueError:
            print("Invalid input. Please enter a valid number.")

def remove_duplicates(main_file, new_file, output_file, normalized_output_file, exclusion_file):
    exclusion_keywords = ["affiliate", "affiliates"]

    try:
        main_df = fetch_google_sheet_data(SHEET_ID, SHEET_NAME)

        new_df = pd.read_csv(new_file)

        if 'URL' not in main_df.columns or 'URL' not in new_df.columns:
            raise KeyError("'URL' column not found in one of the input files.")

        main_df['normalized_domain'] = main_df['URL'].apply(normalize_url)
        main_df['processed_domain'] = main_df['normalized_domain'].apply(process_for_check)

        new_df['normalized_domain'] = new_df['URL'].apply(normalize_url)
        new_df['final_domain'] = resolve_redirects_in_batch(new_df['normalized_domain'])
        new_df['processed_domain'] = new_df['final_domain'].apply(process_for_check)

        new_df = new_df.drop_duplicates(subset='final_domain', keep='first')

        # Deduplicate based on final_domain first
        new_df = new_df.drop_duplicates(subset='final_domain', keep='first')

        # Deduplicate within new_df based on processed_domain (main domain)
        # Prefer entries where final_domain is the main domain (matches processed_domain)
        new_df['is_main_domain'] = new_df['final_domain'] == new_df['processed_domain']
        new_df['domain_length'] = new_df['final_domain'].str.len()
        # Sort to prioritize main domains and shorter domains
        new_df.sort_values(['is_main_domain', 'domain_length'], ascending=[False, True], inplace=True)
        # Keep first occurrence per processed_domain
        new_df = new_df.drop_duplicates(subset='processed_domain', keep='first')
        new_df = new_df.drop(columns=['is_main_domain', 'domain_length'])
        
        exclusion_entries = new_df[new_df['final_domain'].str.contains('|'.join(exclusion_keywords), na=False)]
        exclusion_entries.to_csv(exclusion_file, index=False)
        new_df = new_df[~new_df['final_domain'].isin(exclusion_entries['final_domain'])]

        main_processed_set = set(main_df['processed_domain'])
        main_normalized_set = set(main_df['normalized_domain'])

        main_incremental = {}
        for domain in main_df['normalized_domain']:
            key, lead_num, trail_num = get_incremental_key(domain)  # Capture all 3 values
            if key and (key not in main_incremental or (lead_num, trail_num) > main_incremental[key]):
                main_incremental[key] = (lead_num, trail_num)  # Store both numbers

        new_domains = new_df['final_domain'].tolist()
        new_processed_domains = new_df['processed_domain'].tolist()

        new_records = []
        new_incremental = {}
        for domain, processed_domain in zip(new_domains, new_processed_domains):
            key, lead_num, trail_num = get_incremental_key(domain)  # Capture all 3
            new_records.append({
                'domain': domain, 
                'processed_domain': processed_domain, 
                'key': key, 
                'lead_num': lead_num, 
                'trail_num': trail_num
            })
            if key and (key not in new_incremental or (lead_num, trail_num) > new_incremental[key]):
                new_incremental[key] = (lead_num, trail_num)  # Store both


        duplicate_mask = []
        for record in new_records:
            domain = record['domain']
            processed_domain = record['processed_domain']
            key = record['key']
            lead_num = record['lead_num']
            trail_num = record['trail_num']
            
            if processed_domain in main_processed_set:
                duplicate_mask.append(True)
                continue
                
            if domain in main_normalized_set:
                duplicate_mask.append(True)
                continue
                
            if key:
                main_lead, main_trail = main_incremental.get(key, (-1, -1))
                # Compare both lead and trail numbers
                if (lead_num, trail_num) <= (main_lead, main_trail):
                    duplicate_mask.append(True)
                    continue
                    
                new_lead, new_trail = new_incremental.get(key, (-1, -1))
                if (lead_num, trail_num) < (new_lead, new_trail):
                    duplicate_mask.append(True)
                    continue

            duplicate_mask.append(False)

        unique_entries = new_df[~pd.Series(duplicate_mask, index=new_df.index)]
        unique_entries['Casino Name'] = unique_entries['final_domain'].apply(extract_name_from_domain)

        vpn_choice = select_vpn()

        output_df = pd.DataFrame({  
            'URL': 'https://' + unique_entries['final_domain'],
            'VPN': vpn_choice,
            'Account verified / or not': 'Pending',
            'Assign to': [random.choice(["waleednaeem1100@gmail.com", "msohaibbhai111@gmail.com", "zainjanjua690@gmail.com"]) for _ in range(len(unique_entries))],
            'User': 'import@gc.com'
        })

        output_df.to_csv(output_file, index=False)

        normalized_main_df = pd.DataFrame({
            'url': 'https://' + main_df['normalized_domain']
        })
        normalized_main_df.to_csv(normalized_output_file, index=False)

        return output_df

    except Exception as e:
        logging.error(f"Critical error: {e}")
        return None

if __name__ == "__main__":
    print("Starting the process...")
    new_file = 'Clean Duplicates/clean duplicates from two files/clean_and_process_main_database/Raw_Data.csv'
    output_file = 'Clean Duplicates/clean duplicates from two files/clean_and_process_main_database/cleaned_sheet_Raw_data.csv'
    normalized_output_file = 'Clean Duplicates/clean duplicates from two files/clean_and_process_main_database/normalized_main_file.csv'
    exclusion_file = 'Clean Duplicates/clean duplicates from two files/clean_and_process_main_database/exclusion_entries.csv'

    result = remove_duplicates(None, new_file, output_file, normalized_output_file, exclusion_file)

    if result is not None:
        print(f"Process completed successfully. Output saved to {output_file}.")
    else:
        print("An error occurred during the process. Check debug.log for details.")