import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from deepdiff import DeepDiff  # Install this using `pip install deepdiff`

# Google Sheet and file configuration
SHEET_ID = '1o5kGS8rhHru6wsiSSiXRTsp3P1Id_5SQWt8fyXOkYK8'
EXCEL_FILE = 'Data Management/sync_google_sheet_to_excel/Games Data 2.0 - Local.xlsx'
NEW_SHEET_ID = '1W9QtY3M4hnD5ujcMlgxbfCbfyDZDZWFdLZTf-GNcNPw'  # Replace with your new Google Sheet ID


def make_headers_unique(headers):
    """Make headers unique by appending suffixes to duplicates."""
    seen = {}
    unique_headers = []
    for header in headers:
        if header in seen:
            seen[header] += 1
            unique_headers.append(f"{header}_{seen[header]}")
        else:
            seen[header] = 0
            unique_headers.append(header)
    return unique_headers


def get_google_sheet_modified_time(sheet_id):
    """Get the last modified time of the Google Sheet using google-auth."""
    # Use google-auth to create credentials
    credentials = service_account.Credentials.from_service_account_file(
        'Requirements Files/GOOGLE_CREDENTIALS_FILE.json',
        scopes=['https://www.googleapis.com/auth/drive.metadata.readonly']
    )
    service = build('drive', 'v3', credentials=credentials)

    # Fetch metadata for the Google Sheet
    file_metadata = service.files().get(fileId=sheet_id, fields='modifiedTime').execute()
    return file_metadata['modifiedTime']


def download_google_sheet_to_excel(sheet_id, excel_file):
    """Download Google Sheet to a local Excel file."""
    # Set up credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('Requirements Files/GOOGLE_CREDENTIALS_FILE.json', scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet
    spreadsheet = client.open_by_key(sheet_id)

    # Create a new Excel writer
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        # Loop through all tabs and save them as separate sheets in Excel
        for sheet in spreadsheet.worksheets():
            data = sheet.get_all_values()

            if data:
                headers = data[0]  # First row as headers
                unique_headers = make_headers_unique(headers)
                rows = data[1:]  # All rows except the header row
                df = pd.DataFrame(rows, columns=unique_headers)
            else:
                df = pd.DataFrame()  # Handle empty sheets

            # Save each tab as a separate sheet in the Excel file
            sheet_name = sheet.title
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print(f"Google Sheet has been downloaded to {excel_file}")


def incremental_update_to_google_sheet(excel_file, new_sheet_id):
    """Update only the modified rows in the Google Sheet."""
    # Set up credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('Requirements Files/GOOGLE_CREDENTIALS_FILE.json', scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet
    new_spreadsheet = client.open_by_key(new_sheet_id)

    # Read the Excel file
    df = pd.read_excel(excel_file, sheet_name=None)

    # Loop through each sheet in the Excel file
    for sheet_name, sheet_data in df.items():
        # Replace NaN values with an empty string
        sheet_data = sheet_data.fillna("")

        # Convert datetime columns to strings
        for col in sheet_data.select_dtypes(include=['datetime']).columns:
            sheet_data[col] = sheet_data[col].astype(str)

        # Try to get the worksheet, create it if it doesn't exist
        try:
            worksheet = new_spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = new_spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
            worksheet.update([sheet_data.columns.values.tolist()] + sheet_data.values.tolist())
            print(f"Created new worksheet: {sheet_name}")
            continue

        # Get the current data from the Google Sheet
        existing_data = worksheet.get_all_values()

        # Check if there is any data
        if existing_data:
            existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])  # Skip header row
        else:
            existing_df = pd.DataFrame(columns=sheet_data.columns)

        # Replace NaN and ensure column compatibility
        existing_df = existing_df.reindex(columns=sheet_data.columns, fill_value="").fillna("")

        # Detect changes
        diff = DeepDiff(existing_df.to_dict(orient='records'), sheet_data.to_dict(orient='records'))

        if 'values_changed' in diff or 'dictionary_item_added' in diff or 'dictionary_item_removed' in diff:
            print(f"Updating changes in {sheet_name}...")

            # Update only changed rows
            for index, row in sheet_data.iterrows():
                if index >= len(existing_data) - 1:  # New rows
                    worksheet.append_row(row.values.tolist())
                elif row.to_list() != existing_df.iloc[index].to_list():  # Changed rows

                    worksheet.update(f"A{index + 2}:Z{index + 2}", [row.to_list()])

        print(f"Incremental update completed for {sheet_name}.")


if __name__ == "__main__":
    # Get the last modified time of the Google Sheet
    google_sheet_modified_time = get_google_sheet_modified_time(SHEET_ID)

    # Check if the local file exists
    if os.path.exists(EXCEL_FILE):
        # Get the local file's last modified time
        local_file_modified_time = os.path.getmtime(EXCEL_FILE)
        local_file_modified_time_str = pd.to_datetime(local_file_modified_time, unit='s').isoformat()

        # Compare the timestamps
        if google_sheet_modified_time <= local_file_modified_time_str:
            print("Local file is already up to date.")
        else:
            print("Google Sheet has been updated. Downloading the latest version...")
            download_google_sheet_to_excel(SHEET_ID, EXCEL_FILE)
    else:
        print("Local file does not exist. Downloading the Google Sheet...")
        download_google_sheet_to_excel(SHEET_ID, EXCEL_FILE)

    # Perform incremental update to the new Google Sheet
    incremental_update_to_google_sheet(EXCEL_FILE, NEW_SHEET_ID)
