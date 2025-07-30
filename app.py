import streamlit as st
import pandas as pd
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
import tempfile
import datetime
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import io
import pickle
from google.auth.transport.requests import Request
from pathlib import Path

# Force light theme and other configurations
st.set_page_config(
    page_title="Olgam Plasma Center - Data Processor",
    page_icon="🩸",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for better styling and force light theme
st.markdown("""
    <style>
        /* Force light theme */
        [data-testid="stAppViewContainer"] {
            background-color: #ffffff;
        }
        
        [data-testid="stSidebar"] {
            background-color: #f8f9fa;
        }
        
        .main {
            padding: 0rem 1rem;
            background-color: #ffffff;
        }
        
        .stTitle {
            font-size: 3rem;
            color: #2c3e50;
            padding-bottom: 1rem;
        }
        
        .stAlert {
            padding: 1rem;
            margin: 1rem 0;
            border-radius: 0.5rem;
        }
        
        .css-1v0mbdj.ebxwdo61 {
            margin-top: 2rem;
        }
        
        /* Ensure text is dark */
        .stMarkdown {
            color: #2c3e50;
        }
        
        /* Style metrics */
        [data-testid="stMetricValue"] {
            color: #2c3e50;
            background-color: #ffffff;
        }
        
        /* Style dataframe */
        .stDataFrame {
            background-color: #ffffff;
        }
        
        /* Style buttons */
        .stButton button {
            background-color: #2c3e50;
            color: #ffffff;
            border-radius: 0.5rem;
        }
        
        /* Style file uploader */
        [data-testid="stFileUploader"] {
            background-color: #f8f9fa;
            padding: 1rem;
            border-radius: 0.5rem;
        }
        
        /* Style expander */
        .streamlit-expanderHeader {
            background-color: #f8f9fa;
            color: #2c3e50;
        }
    </style>
""", unsafe_allow_html=True)

# Password protection
def check_password():
    """Returns `True` if the user had the correct password."""
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password
        st.markdown("""
            <div style='text-align: center; padding: 1rem;'>
                <h1 style='color: #2c3e50;'>🩸 Olgam Plasma Center</h1>
                <h2 style='color: #7f8c8d;'>Database Processor</h2>
            </div>
        """, unsafe_allow_html=True)
        
        st.text_input(
            "Please enter the password to access the application",
            type="password",
            on_change=password_entered,
            key="password"
        )
        return False
    
    return st.session_state["password_correct"]

def get_google_auth_url():
    flow = InstalledAppFlow.from_client_config(
        {
            "web": {
                "client_id": st.secrets["google_oauth"]["client_id"],
                "project_id": st.secrets["google_oauth"]["project_id"],
                "auth_uri": st.secrets["google_oauth"]["auth_uri"],
                "token_uri": st.secrets["google_oauth"]["token_uri"],
                "auth_provider_x509_cert_url": st.secrets["google_oauth"]["auth_provider_x509_cert_url"],
                "client_secret": st.secrets["google_oauth"]["client_secret"],
                "redirect_uris": ["https://olgamdb.streamlit.app/"]
            }
        },
        scopes=['https://www.googleapis.com/auth/drive.file'],
        redirect_uri="https://olgamdb.streamlit.app/"
    )
    auth_url, _ = flow.authorization_url(prompt='consent')
    return auth_url

def get_google_creds():
    creds = None
    
    # Check if we have a token in session state
    if 'google_token' in st.session_state:
        creds = st.session_state['google_token']
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Get the authorization code from URL parameters
            params = st.experimental_get_query_params()
            if 'code' in params:
                try:
                    flow = InstalledAppFlow.from_client_config(
                        {
                            "web": {
                                "client_id": st.secrets["google_oauth"]["client_id"],
                                "project_id": st.secrets["google_oauth"]["project_id"],
                                "auth_uri": st.secrets["google_oauth"]["auth_uri"],
                                "token_uri": st.secrets["google_oauth"]["token_uri"],
                                "auth_provider_x509_cert_url": st.secrets["google_oauth"]["auth_provider_x509_cert_url"],
                                "client_secret": st.secrets["google_oauth"]["client_secret"],
                                "redirect_uris": ["https://olgamdb.streamlit.app/"]
                            }
                        },
                        scopes=['https://www.googleapis.com/auth/drive.file'],
                        redirect_uri="https://olgamdb.streamlit.app/"
                    )
                    code = params['code'][0]
                    flow.fetch_token(code=code)
                    creds = flow.credentials
                    st.session_state['google_token'] = creds
                    
                    # Clear the URL parameters to avoid reusing the same code
                    st.experimental_set_query_params()
                    st.success("✅ Successfully authenticated with Google Drive!")
                    st.info("Please continue with your file upload now.")
                except Exception as e:
                    st.error(f"Authentication error: {str(e)}")
                    st.info("Please try authorizing again.")
                    auth_url = get_google_auth_url()
                    st.markdown(f'''
                        <h3>🔐 Google Drive Authorization Required</h3>
                        <p>To save excess files to your Google Drive, please authorize the application:</p>
                        <a href="{auth_url}" target="_blank">
                            <button style="
                                background-color: #4285f4;
                                color: white;
                                padding: 10px 20px;
                                border: none;
                                border-radius: 5px;
                                cursor: pointer;
                                font-size: 16px;
                                margin: 10px 0;">
                                🔑 Authorize Google Drive Access
                            </button>
                        </a>
                        ''', unsafe_allow_html=True)
                    st.stop()
            else:
                auth_url = get_google_auth_url()
                st.markdown(f'''
                    <h3>🔐 Google Drive Authorization Required</h3>
                    <p>To save excess files to your Google Drive, please authorize the application:</p>
                    <ol>
                        <li>Click the button below to authorize</li>
                        <li>Sign in with your Google account</li>
                        <li>Grant the requested permissions</li>
                        <li>You will be redirected back to this app</li>
                    </ol>
                    <a href="{auth_url}" target="_blank">
                        <button style="
                            background-color: #4285f4;
                            color: white;
                            padding: 10px 20px;
                            border: none;
                            border-radius: 5px;
                            cursor: pointer;
                            font-size: 16px;
                            margin: 10px 0;">
                            🔑 Authorize Google Drive Access
                        </button>
                    </a>
                    <p style="color: #666; margin-top: 20px;">Note: After authorization, you will be redirected to <strong>https://olgamdb.streamlit.app/</strong>. Please return to this tab if it doesn't automatically.</p>
                    ''', unsafe_allow_html=True)
                st.stop()
    
    return creds

# Define the required columns
REQUIRED_COLUMNS = [
    'Facility', 'Donor #', 'Donor Name', 'Donor E-mail', 'Donor Account #',
    'Donor Phone', 'Yield (ml)', 'Gender', 'Donation Date', 'Month',
    'Hour Checked In', 'Day Of The Week', 'Age', 'Check-In Time',
    'Check-Out Time (Adjusted)', 'Visit mins. (Adjusted)', 'Donor Address Line 1',
    'Donor Address Line 2', 'City', 'Zip Code', 'Donor Status', 'Qual. Status',
    'Last 	Donation Date', 'Pure Plasma', 'Target Volume'
]

def format_phone(phone):
    if pd.isna(phone):
        return phone
    # Remove all non-numeric characters
    numbers = re.sub(r'\D', '', str(phone))
    
    # If length is 10, add '1' prefix
    if len(numbers) == 10:
        numbers = '1' + numbers
    # If length is not 11 after processing, return original
    if len(numbers) != 11:
        return phone
    
    # Format to 1(XXX) XXX-XXXX
    return f"1({numbers[1:4]}) {numbers[4:7]}-{numbers[7:]}"

def process_name(name):
    if pd.isna(name):
        return '', ''
    
    # Split by comma
    parts = name.split(',', 1)
    if len(parts) == 2:
        last_name, first_name = parts
    else:
        # If no comma, assume it's all first name
        first_name = parts[0]
        last_name = ''
    
    # Clean and title case each word
    first_name = ' '.join(word.strip().lower().capitalize() for word in first_name.split())
    last_name = ' '.join(word.strip().lower().capitalize() for word in last_name.split())
    
    return first_name, last_name

def process_data(df):
    # First, convert 'Last Donation Date' to datetime for proper comparison
    df['Last 	Donation Date'] = pd.to_datetime(df['Last 	Donation Date'], errors='coerce')
    
    # Sort by 'Last Donation Date' in descending order and remove duplicates
    # keeping the first occurrence (which will be the most recent due to sorting)
    # Now considering both Donor # and Facility for duplicates
    df = df.sort_values('Last 	Donation Date', ascending=False).drop_duplicates(subset=['Donor #', 'Facility'])
    
    # Create a new DataFrame with only required columns
    processed_df = pd.DataFrame()
    
    # Copy basic columns and preserve Donor # exactly as is
    processed_df['Donor #'] = df['Donor #']  # No need for extra conversion since it's already a string
    
    processed_df['Donor Account #'] = df['Donor Account #']
    processed_df['Zip Code'] = df['Zip Code']
    processed_df['Donor Status'] = df['Donor Status']
    processed_df['Facility'] = df['Facility']
    
    # Extract DOB (Birthday) from column DOB if it exists
    if 'DOB' in df.columns:
        processed_df['Birthday'] = df['DOB']
    
    # Process names
    names = df['Donor Name'].apply(process_name)
    processed_df['Donor First'] = names.apply(lambda x: x[0])
    processed_df['Donor Last'] = names.apply(lambda x: x[1])
    
    # Process email
    processed_df['Donor E-mail'] = df['Donor E-mail'].str.lower()
    # Remove both types of invalid emails
    invalid_emails = ['someone@plasmaworld.com', 'someone@plasma.com', 'some@plasmaworld.com','someone@plasmaworld.om', 'na@na.com', 'someoneinplasma@gmail.com', 'someoneinplasma@gmail.com']
    processed_df.loc[processed_df['Donor E-mail'].isin(invalid_emails), 'Donor E-mail'] = None
    
    # Process phone
    processed_df['Donor Phone'] = df['Donor Phone'].apply(format_phone)
    
    # Combine addresses
    processed_df['Donor Address'] = df['Donor Address Line 1'].fillna('') + ' ' + df['Donor Address Line 2'].fillna('')
    processed_df['Donor Address'] = processed_df['Donor Address'].str.strip()

    #reset index
    processed_df = processed_df.reset_index(drop=True)
    
    return processed_df

def validate_file(file):
    # Check file extension
    if not file.name.endswith('.xlsx'):
        return False, "Please upload an Excel file (.xlsx)", None
    
    try:
        # Read the Excel file with Donor # as string
        df = pd.read_excel(
            file,
            dtype={'Donor #': str}  # Force Donor # to be read as string
        )
        
        # Get the columns from the uploaded file
        file_columns = df.columns.tolist()
        
        # Check if all required columns are present
        missing_columns = [col for col in REQUIRED_COLUMNS if col not in file_columns]
        
        if missing_columns:
            return False, f"Missing required columns: {', '.join(missing_columns)}", None
        
        return True, "File structure is valid!", df
    
    except Exception as e:
        return False, f"Error reading file: {str(e)}", None

def load_master_db():
    """Load the master database from Google Sheets."""
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        
        # Create the credentials dictionary
        credentials_dict = {
            "type": "service_account",
            "project_id": "third-hangout-387516",
            "private_key_id": st.secrets["private_key_id"],
            "private_key": st.secrets["google_credentials"],
            "client_email": "apollo-miner@third-hangout-387516.iam.gserviceaccount.com",
            "client_id": "114223947184571105588",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/apollo-miner%40third-hangout-387516.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        }
        
        # Use the dictionary directly with from_json_keyfile_dict
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
        gc = gspread.authorize(credentials)
        
        # Use the spreadsheet key from secrets
        spreadsheet_key = st.secrets["spreadsheet_key"]
        workbook = gc.open_by_key(spreadsheet_key)
        worksheet = workbook.worksheet('COMBINED')
        all_values = worksheet.get_all_values()
        headers = all_values[0][:10]
        data = [row[:10] for row in all_values[1:]]
        master_df = pd.DataFrame(data, columns=headers)
        return master_df
    except Exception as e:
        st.error(f"Error loading master database: {str(e)}")
        return None

def compare_dataframes(processed_df, master_df):
    """Compare processed data with master database to find new and updated records."""
    # Ensure both dataframes have the same column names
    master_df.columns = ['Donor #', 'Donor First', 'Donor Last', 'Donor E-mail', 
                        'Donor Account #', 'Donor Phone', 'Donor Address', 
                        'Zip Code', 'Donor Status', 'Center']
    
    # Convert master_df Donor # to string for comparison
    master_df['Donor #'] = master_df['Donor #'].astype(str)
    
    # Create a composite key for comparison
    processed_df['composite_key'] = processed_df['Donor #'] + '_' + processed_df['Facility']
    master_df['composite_key'] = master_df['Donor #'] + '_' + master_df['Center']
    
    # Find new records (donors that don't exist in master_df based on composite key)
    new_donors = processed_df[~processed_df['composite_key'].isin(master_df['composite_key'])]
    
    # For existing donors, check for updates
    existing_donors = processed_df[processed_df['composite_key'].isin(master_df['composite_key'])]
    
    # Create a copy of existing_donors with 'Facility' renamed to 'Center' for comparison
    existing_donors_comp = existing_donors.copy()
    existing_donors_comp['Center'] = existing_donors_comp['Facility']
    
    # Merge to compare differences using composite key
    comparison_df = existing_donors_comp.merge(
        master_df,
        on='composite_key',
        how='left',
        suffixes=('_new', '_master')
    )
    
    # Function to standardize values for comparison
    def standardize_value(x):
        if pd.isna(x):
            return ''
        # Convert to string and clean
        x = str(x).lower().strip()
        # Remove all spaces, special characters, and punctuation
        x = re.sub(r'[^a-z0-9@.]', '', x)
        return x
    
    # Apply standardization to relevant fields
    fields_to_compare = ['Donor E-mail', 'Donor Phone', 'Donor Address', 'Center']
    for field in fields_to_compare:
        comparison_df[f'{field}_new'] = comparison_df[f'{field}_new'].apply(standardize_value)
        comparison_df[f'{field}_master'] = comparison_df[f'{field}_master'].apply(standardize_value)
    
    # Check for changes in specific fields (ignoring format)
    updated_mask = (
        (comparison_df['Donor E-mail_new'] != comparison_df['Donor E-mail_master']) |
        (comparison_df['Donor Phone_new'] != comparison_df['Donor Phone_master']) |
        (comparison_df['Donor Address_new'] != comparison_df['Donor Address_master']) |
        (comparison_df['Center_new'] != comparison_df['Center_master'])
    )
    
    # Get updated records using the correct column name (Donor #_new)
    updated_donors = existing_donors[existing_donors['Donor #'].isin(
        comparison_df[updated_mask]['Donor #_new']
    )]
    
    # Create really_updated DataFrame for records with specific changes
    really_updated = existing_donors[existing_donors['Donor #'].isin(
        comparison_df[
            (comparison_df['Donor E-mail_new'] != comparison_df['Donor E-mail_master']) |
            (comparison_df['Donor Phone_new'] != comparison_df['Donor Phone_master']) |
            (comparison_df['Donor Address_new'] != comparison_df['Donor Address_master']) |
            (comparison_df['Center_new'] != comparison_df['Center_master'])
        ]['Donor #_new']
    )]
    
    # Remove the temporary composite key columns before returning
    new_donors = new_donors.drop('composite_key', axis=1)
    
    # Clean up comparison_df before using it for updates
    comparison_df = comparison_df.drop(['composite_key'], axis=1)
    
    return new_donors, updated_donors, really_updated

def update_master_database(master_df, new_donors, really_updated):
    """Update master database with new and updated records."""
    # Define the correct column order
    SHEET_COLUMNS = [
        'Donor #', 'Donor First', 'Donor Last', 'Donor E-mail', 'Donor Account #',
        'Donor Phone', 'Donor Address', 'Zip Code', 'Donor Status', 'Center'
    ]
    
    # Create a copy of master_df to avoid modifying the original
    updated_master_df = master_df.copy()
    
    # Remove records that will be updated
    if not really_updated.empty:
        updated_master_df = updated_master_df[~updated_master_df['Donor #'].isin(really_updated['Donor #'])]
    
    # Prepare really_updated records for concatenation
    if not really_updated.empty:
        really_updated_formatted = really_updated.copy()
        really_updated_formatted['Center'] = really_updated_formatted['Facility']
        really_updated_formatted = really_updated_formatted.drop('Facility', axis=1)
    
    # Prepare new_donors records for concatenation
    if not new_donors.empty:
        new_donors_formatted = new_donors.copy()
        new_donors_formatted['Center'] = new_donors_formatted['Facility']
        new_donors_formatted = new_donors_formatted.drop('Facility', axis=1)
    
    # Concatenate the dataframes
    frames_to_concat = [updated_master_df]
    if not really_updated.empty:
        frames_to_concat.append(really_updated_formatted)
    if not new_donors.empty:
        frames_to_concat.append(new_donors_formatted)
    
    final_master_df = pd.concat(frames_to_concat, ignore_index=True)
    
    # Reorder columns to match Google Sheets
    final_master_df = final_master_df[SHEET_COLUMNS]
    
    return final_master_df

def save_to_gsheets(df, worksheet):
    """Save dataframe to Google Sheets."""
    try:
        # Clear existing content
        worksheet.clear()
        
        # Log data types before processing
        st.write("DEBUG: Data types in save_to_gsheets:")
        for col in df.columns:
            st.write(f"  {col}: {df[col].dtype}")
        
        # Convert any datetime/timestamp columns to strings
        df_processed = df.copy()
        for col in df_processed.columns:
            if df_processed[col].dtype == 'datetime64[ns]' or str(df_processed[col].dtype).startswith('datetime'):
                st.write(f"DEBUG: Converting {col} from datetime to string in save_to_gsheets")
                df_processed[col] = df_processed[col].dt.strftime('%Y-%m-%d').fillna('')
        
        # Replace NaN values with empty strings
        df_clean = df_processed.fillna('')
        
        # Log sample data
        st.write("DEBUG: Sample data being saved to Google Sheets:")
        st.write(df_clean.head())
        
        # Update with new content
        worksheet.update([df_clean.columns.values.tolist()] + df_clean.values.tolist())
        st.success(f"Successfully saved {len(df_clean)} records to Google Sheets")
        return True
    except Exception as e:
        st.error(f"Error saving to Google Sheets: {str(e)}")
        st.error(f"Error type: {type(e).__name__}")
        import traceback
        st.error(f"Full traceback: {traceback.format_exc()}")
        return False

def get_leads_for_upload(new_donors, really_updated, master_df):
    """Create a dataframe of leads that need to be uploaded (new or updated phone/email)."""
    # Initialize empty dataframe for leads
    leads_df = pd.DataFrame()
    
    # Function to check if phone or email was updated
    def has_important_updates(row, master_df):
        if row['Donor #'] not in master_df['Donor #'].values:
            return True  # New donor
        
        master_record = master_df[master_df['Donor #'] == row['Donor #']].iloc[0]
        
        # Standardize values for comparison
        new_phone = str(row['Donor Phone']).lower().strip() if pd.notna(row['Donor Phone']) else ''
        new_email = str(row['Donor E-mail']).lower().strip() if pd.notna(row['Donor E-mail']) else ''
        master_phone = str(master_record['Donor Phone']).lower().strip() if pd.notna(master_record['Donor Phone']) else ''
        master_email = str(master_record['Donor E-mail']).lower().strip() if pd.notna(master_record['Donor E-mail']) else ''
        
        return new_phone != master_phone or new_email != master_email
    
    # Combine new donors and really updated records
    if not new_donors.empty:
        leads_df = pd.concat([leads_df, new_donors])
    
    if not really_updated.empty:
        # Filter really_updated for only those with phone or email changes
        important_updates = really_updated[really_updated.apply(lambda x: has_important_updates(x, master_df), axis=1)]
        leads_df = pd.concat([leads_df, important_updates])
    
    return leads_df.reset_index(drop=True)

def append_to_upload_process(new_donors, really_updated):
    """Append new and updated records to UPLOAD_PROCESS worksheet."""
    try:
        # Setup Google Sheets connection
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        credentials_dict = {
            "type": "service_account",
            "project_id": "third-hangout-387516",
            "private_key_id": st.secrets["private_key_id"],
            "private_key": st.secrets["google_credentials"],
            "client_email": "apollo-miner@third-hangout-387516.iam.gserviceaccount.com",
            "client_id": "114223947184571105588",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/apollo-miner%40third-hangout-387516.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        }
        # Use the dictionary directly with from_json_keyfile_dict
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
        gc = gspread.authorize(credentials)
        spreadsheet_key = st.secrets["spreadsheet_key"]
        workbook = gc.open_by_key(spreadsheet_key)
        worksheet = workbook.worksheet('UPLOAD_PROCESS')

        # Define the correct column order
        UPLOAD_COLUMNS = [
            'Donor #', 'Donor First', 'Donor Last', 'Donor E-mail', 'Donor Account #',
            'Donor Phone', 'Donor Address', 'Zip Code', 'Donor Status', 'Center', 
            'K', 'L', 'M', 'N', 'Birthday'
        ]

        # Combine new and updated records
        records_to_append = pd.concat([new_donors, really_updated], ignore_index=True)
        if records_to_append.empty:
            st.info("No records to append to UPLOAD_PROCESS")
            return True

        # Log data types before processing
        st.write("DEBUG: Data types before processing:")
        for col in records_to_append.columns:
            st.write(f"  {col}: {records_to_append[col].dtype}")

        # Rename Facility to Center if it exists
        if 'Facility' in records_to_append.columns:
            records_to_append['Center'] = records_to_append['Facility']
            records_to_append = records_to_append.drop('Facility', axis=1)
            
        # Add columns K, L, M, N with 'x' values
        records_to_append['K'] = 'x'
        records_to_append['L'] = 'x'
        records_to_append['M'] = 'x'
        records_to_append['N'] = 'x'
        
        # If Birthday column doesn't exist, create it as empty
        if 'Birthday' not in records_to_append.columns:
            records_to_append['Birthday'] = ''

        # Convert any datetime/timestamp columns to strings
        for col in records_to_append.columns:
            if records_to_append[col].dtype == 'datetime64[ns]' or str(records_to_append[col].dtype).startswith('datetime'):
                st.write(f"DEBUG: Converting {col} from datetime to string")
                records_to_append[col] = records_to_append[col].dt.strftime('%Y-%m-%d').fillna('')

        # Reorder columns to match required order
        records_to_append = records_to_append[UPLOAD_COLUMNS]

        # Prepare records for upload (replace NaN with empty string)
        records_clean = records_to_append.fillna('')
        
        # Log data types after cleaning
        st.write("DEBUG: Data types after cleaning:")
        for col in records_clean.columns:
            st.write(f"  {col}: {records_clean[col].dtype}")
        
        # Check for any remaining non-serializable objects
        st.write("DEBUG: Sample data to be uploaded:")
        st.write(records_clean.head())
        
        # Get the last row with data
        last_row = len(worksheet.get_all_values())
        
        # Convert to list and check each value
        data_to_upload = records_clean.values.tolist()
        st.write(f"DEBUG: Number of rows to upload: {len(data_to_upload)}")
        
        # Append new records starting from the next row
        worksheet.append_rows(
            data_to_upload,
            value_input_option='RAW',
            insert_data_option='INSERT_ROWS',
            table_range=f'A{last_row + 1}'
        )
        
        st.success(f"Successfully appended {len(data_to_upload)} records to UPLOAD_PROCESS")
        return True
    except Exception as e:
        st.error(f"Error appending to UPLOAD_PROCESS: {str(e)}")
        st.error(f"Error type: {type(e).__name__}")
        import traceback
        st.error(f"Full traceback: {traceback.format_exc()}")
        return False



def save_excel_to_drive_personal(df, filename, folder_id=None):
    # Get Google credentials
    creds = get_google_creds()
    service = build('drive', 'v3', credentials=creds)
    
    # Save the DataFrame as a temporary Excel file
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            temp_file = tmp.name
            df.to_excel(temp_file, index=False)
        
        file_metadata = {'name': filename, 'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}
        if folder_id:
            file_metadata['parents'] = [folder_id]
        media = MediaFileUpload(temp_file, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        file = service.files().create(body=file_metadata, media_body=media, fields='id,webViewLink').execute()
        return file.get('webViewLink')
    except Exception as e:
        raise e
    finally:
        # Clean up temporary file with retry mechanism
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except OSError:
                # If immediate deletion fails, try again after a short delay
                import time
                time.sleep(0.1)
                try:
                    os.unlink(temp_file)
                except OSError:
                    # If still fails, just log it but don't raise error
                    st.warning(f"Could not delete temporary file: {temp_file}")

def upload_raw_to_gsheet(df):
    """Uploads the validated original DataFrame to the specified Google Sheets worksheet, adding to the end. If the cell limit is exceeded, excess files are uploaded as Excel (.xlsx) to your personal Google Drive using OAuth, in parts of maximum 50,000 rows."""
    try:
        import gspread
        from gspread.exceptions import APIError
        
        # --- NUEVO BLOQUE: Eliminar la 5ta columna si corresponde ---
        df_to_upload = df.copy()
        if df_to_upload.shape[1] >= 5:
            fifth_col = df_to_upload.columns[4]
            fifth_col_lower = fifth_col.strip().replace(' ', '').lower()
            # Variaciones aceptadas para nombre de columna
            dob_variants = [
                'dob', 'dateofbirth', 'birthdate', 'birth', 'fechadenacimiento', 'fecha_nacimiento', 'nacimiento'
            ]
            # Si el nombre coincide con alguna variante
            if any(variant in fifth_col_lower for variant in dob_variants):
                df_to_upload = df_to_upload.drop(columns=[fifth_col])
            # O si la columna es de tipo fecha
            elif pd.api.types.is_datetime64_any_dtype(df_to_upload.iloc[:, 4]):
                df_to_upload = df_to_upload.drop(columns=[fifth_col])
            # O si la mayoría de los valores parecen fechas (por ejemplo, más del 80% se pueden convertir a fecha)
            else:
                date_count = pd.to_datetime(df_to_upload.iloc[:, 4], errors='coerce').notna().sum()
                if date_count / len(df_to_upload) > 0.8:
                    df_to_upload = df_to_upload.drop(columns=[fifth_col])
        # --- FIN BLOQUE NUEVO ---
        
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        credentials_dict = {
            "type": "service_account",
            "project_id": "third-hangout-387516",
            "private_key_id": st.secrets["private_key_id"],
            "private_key": st.secrets["google_credentials"],
            "client_email": "apollo-miner@third-hangout-387516.iam.gserviceaccount.com",
            "client_id": "114223947184571105588",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/apollo-miner%40third-hangout-387516.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        }
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
        gc = gspread.authorize(credentials)
        workbook = gc.open_by_key('1t2PAePYWTpDQbPTlafIhSUUdiF_CJKDnSUfMc63zoX0')
        worksheet = workbook.get_worksheet(0)  # Primera hoja
        
        # Convertir columnas de tipo fecha/hora y time a string
        df_processed = df_to_upload.copy()
        for col in df_processed.columns:
            if pd.api.types.is_datetime64_any_dtype(df_processed[col]):
                df_processed[col] = df_processed[col].dt.strftime('%Y-%m-%d').fillna('')
            elif pd.api.types.is_timedelta64_dtype(df_processed[col]):
                df_processed[col] = df_processed[col].astype(str)
            elif pd.api.types.is_object_dtype(df_processed[col]):
                if df_processed[col].apply(lambda x: hasattr(x, 'isoformat')).any():
                    df_processed[col] = df_processed[col].apply(lambda x: x.isoformat() if hasattr(x, 'isoformat') else str(x))
        
        df_clean = df_processed.fillna('')
        data_to_upload = df_clean.values.tolist()
        n_rows_to_insert = len(data_to_upload)
        n_cols = len(df_clean.columns)
        current_rows = len(worksheet.get_all_values())
        used_cells = current_rows * n_cols
        max_cells = 10_000_000
        available_cells = max_cells - used_cells
        max_rows_to_insert = available_cells // n_cols
        start_idx = 0
        inserted_in_main = False
        
        if n_rows_to_insert <= max_rows_to_insert:
            last_row = current_rows
            try:
                worksheet.append_rows(
                    data_to_upload,
                    value_input_option='RAW',
                    insert_data_option='INSERT_ROWS',
                    table_range=f'A{last_row + 1}'
                )
                inserted_in_main = True
            except APIError as api_err:
                st.warning("Could not insert into main sheet due to cell limit. The rest will be saved in new files.")
                start_idx = 0
        else:
            if max_rows_to_insert > 0:
                data_fit = data_to_upload[:max_rows_to_insert]
                last_row = current_rows
                try:
                    worksheet.append_rows(
                        data_fit,
                        value_input_option='RAW',
                        insert_data_option='INSERT_ROWS',
                        table_range=f'A{last_row + 1}'
                    )
                    start_idx = max_rows_to_insert
                    inserted_in_main = True
                except APIError as api_err:
                    st.warning("Could not insert into main sheet due to cell limit. The rest will be saved in new files.")
                    start_idx = 0
            else:
                start_idx = 0
        
        # Now, create Excel files with maximum 50,000 rows and upload them to your personal Drive
        file_count = 1
        data_rest = data_to_upload[start_idx:] if not inserted_in_main else data_to_upload[start_idx:]
        folder_id = '1x5UQxQ1tIf6Blzvin9PGldrMIPl6Qyee'
        max_excel_rows = 50000
        
        while data_rest:
            data_chunk = data_rest[:max_excel_rows]
            data_rest = data_rest[max_excel_rows:]
            df_chunk = pd.DataFrame(data_chunk, columns=df_clean.columns)
            new_title = f"Olgam_Data_Excess_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_part{file_count}.xlsx"
            try:
                link = save_excel_to_drive_personal(df_chunk, new_title, folder_id=folder_id)
                st.success(f"✅ Part {file_count} of the data was saved to your Google Drive")
                st.markdown(f"[Open file in Google Drive]({link})", unsafe_allow_html=True)
            except Exception as move_err:
                error_msg = str(move_err)
                if "could not locate runnable browser" in error_msg or "redirect_uri_mismatch" in error_msg or "invalid_grant" in error_msg:
                    st.error("Google Drive authentication required")
                    # Get fresh credentials
                    try:
                        creds = get_google_creds()
                        # Try again with new credentials
                        link = save_excel_to_drive_personal(df_chunk, new_title, folder_id=folder_id)
                        st.success(f"✅ Part {file_count} of the data was saved to your Google Drive")
                        st.markdown(f"[Open file in Google Drive]({link})", unsafe_allow_html=True)
                    except Exception as auth_err:
                        st.error(f"Authentication failed: {str(auth_err)}")
                        # Try to save locally as fallback
                        try:
                            local_filename = f"Olgam_Data_Excess_Part_{file_count}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                            df_chunk.to_excel(local_filename, index=False)
                            st.info(f"📁 File part {file_count} saved locally as: {local_filename}")
                        except Exception as local_err:
                            st.error(f"Failed to save file part {file_count} locally: {local_err}")
                else:
                    st.warning(f"Could not upload file part {file_count} to your Google Drive: {move_err}")
                    # Try to save locally as fallback
                    try:
                        local_filename = f"Olgam_Data_Excess_Part_{file_count}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                        df_chunk.to_excel(local_filename, index=False)
                        st.info(f"📁 File part {file_count} saved locally as: {local_filename}")
                    except Exception as local_err:
                        st.error(f"Failed to save file part {file_count} locally: {local_err}")
            file_count += 1
        
        return True
    except Exception as e:
        st.error(f"Error uploading original data to Google Sheets: {str(e)}")
        import traceback
        st.error(f"Full traceback: {traceback.format_exc()}")
        return False

def main():
    if not check_password():
        st.error("⚠️ Password incorrect. Please try again.")
        return

    # Header with logo and title
    st.markdown("""
        <div style='text-align: center; padding: 1rem;'>
            <h1 style='color: #2c3e50;'>🩸 Olgam Plasma Center</h1>
            <h2 style='color: #7f8c8d;'>Database Processor</h2>
        </div>
    """, unsafe_allow_html=True)
    
    # File uploader with custom styling
    uploaded_file = st.file_uploader("Choose an Excel file", type=['xlsx'])
    
    if uploaded_file is not None:
        with st.spinner("Processing data and updating databases..."):
            # Validate the file
            is_valid, message, df = validate_file(uploaded_file)
            if is_valid:
                # Upload original data to external Google Sheets
                success_raw = upload_raw_to_gsheet(df)
                if not success_raw:
                    st.error("Could not upload original file to external sheet. Process stopped.")
                    return
                # Store initial record count
                initial_records = len(df)
                
                # Process the data
                processed_df = process_data(df)
                
                # Load master database
                master_df = load_master_db()
                if master_df is None:
                    st.error("Failed to load master database. Please check the connection.")
                    return
                
                # Compare with master database
                new_donors, updated_donors, really_updated = compare_dataframes(processed_df, master_df)
                
                # Update master database
                final_master_df = update_master_database(master_df, new_donors, really_updated)
                
                # Get leads for upload
                leads_df = get_leads_for_upload(new_donors, really_updated, master_df)
                
                # Save to databases
                scope = ['https://spreadsheets.google.com/feeds',
                        'https://www.googleapis.com/auth/drive']
                credentials_dict = {
                    "type": "service_account",
                    "project_id": "third-hangout-387516",
                    "private_key_id": st.secrets["private_key_id"],
                    "private_key": st.secrets["google_credentials"],
                    "client_email": "apollo-miner@third-hangout-387516.iam.gserviceaccount.com",
                    "client_id": "114223947184571105588",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/apollo-miner%40third-hangout-387516.iam.gserviceaccount.com",
                    "universe_domain": "googleapis.com"
                }
                
                # Use the dictionary directly with from_json_keyfile_dict
                credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
                gc = gspread.authorize(credentials)
                spreadsheet_key = st.secrets["spreadsheet_key"]
                workbook = gc.open_by_key(spreadsheet_key)
                
                # Save to master DB
                worksheet = workbook.worksheet('DB')
                success_master = save_to_gsheets(final_master_df, worksheet)
                
                # Save to upload process
                success_upload = append_to_upload_process(new_donors, really_updated)
                
                if success_master and success_upload:
                    st.success("✅ All databases updated successfully!")
                    
                    # Display statistics
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Total Records", initial_records)
                    with col2:
                        st.metric("Unique Donors", len(processed_df['Donor #'].unique()))
                    with col3:
                        st.metric("New Donors", len(new_donors))
                    with col4:
                        st.metric("Updated Records", len(really_updated))
                    with col5:
                        st.metric("Leads to Upload", len(leads_df))
                    
                    # Show summary of changes
                    if not new_donors.empty:
                        st.markdown(f"### 🆕 New Donors: {len(new_donors)} records")
                        st.write(f"Donor numbers: {', '.join(new_donors['Donor #'].astype(str))}")
                    
                    if not really_updated.empty:
                        st.markdown(f"### 🔄 Updated Records: {len(really_updated)} records")
                        st.write(f"Donor numbers: {', '.join(really_updated['Donor #'].astype(str))}")
                    
                    # Download options only for changed data
                    if not leads_df.empty:
                        st.markdown("### 📥 Download Options")
                        csv_leads = leads_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Leads for Upload (CSV)",
                            data=csv_leads,
                            file_name="Olgam_Leads_For_Upload.csv",
                            mime="text/csv",
                            help="Download leads that need to be uploaded (new or updated phone/email)"
                        )
                else:
                    st.error("❌ Some updates failed. Please check the logs.")
            else:
                st.error("❌ " + message)

if __name__ == "__main__":
    main() 
