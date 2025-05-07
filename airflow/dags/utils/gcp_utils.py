from gspread_dataframe import set_with_dataframe
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def authenticate_google_sheets(credentials_path):
    try:
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"GCP credentials file not found at: {credentials_path}")

        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        
        credentials = Credentials.from_service_account_file(
            credentials_path, scopes=scopes
        )
        
        gc = gspread.authorize(credentials)
        logger.info("Successfully authenticated with Google Sheets API")
        return gc
    
    except Exception as e:
        logger.error(f"Error authenticating with Google Sheets: {str(e)}")
        raise


def update_google_sheet(gcp, data, spreadsheet_id):
    try:
        if not data:
            return True
        
        if not spreadsheet_id:
            raise ValueError("SPREADSHEET_ID environment variable not set")
        
        df = pd.DataFrame(data)

        try:
            spreadsheet = gcp.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.sheet1
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID {spreadsheet_id} not found")
            raise

        if worksheet.row_count <= 1:
            headers = df.columns.tolist()
            worksheet.append_row(headers)
        
        set_with_dataframe(
            worksheet,
            df,
            include_index=False,
            include_column_header=False,
            resize=True,
            row=worksheet.row_count
        )
        
        return True
    except Exception as e:
        raise
