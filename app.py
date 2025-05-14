import streamlit as st
import pandas as pd
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from datetime import datetime

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

# Define the required columns
REQUIRED_COLUMNS = [
    'Facility', 'Donor #', 'Donor Name', 'Donor E-mail', 'Donor Account #',
    'Donor Phone', 'Yield (ml)', 'Gender', 'Donation Date', 'Month',
    'Hour Checked In', 'Day Of The Week', 'Age', 'Check-In Time',
    'Check-Out Time (Adjusted)', 'Visit mins. (Adjusted)', 'Donor Address Line 1',
    'Donor Address Line 2', 'City', 'Zip Code', 'Donor Status', 'Qual. Status',
    'Last 	Donation Date', 'Pure Plasma', 'Target Volume'
]

# Define patterns for automatic column detection
DONOR_NAME_PATTERNS = ['donor name', 'name', 'donor', 'full name']
DONOR_EMAIL_PATTERNS = ['donor e-mail', 'email', 'donor email', 'e-mail']
DONOR_ACCOUNT_PATTERNS = ['donor account #', 'account #', 'account number', 'donor acc']
DONOR_PHONE_PATTERNS = ['donor phone', 'phone', 'telephone', 'contact', 'phone number']
DONOR_NUMBER_PATTERNS = ['donor #', 'donor number', 'id', 'donor id']
FACILITY_PATTERNS = ['facility', 'center', 'location', 'plasma center']
ADDRESS1_PATTERNS = ['donor address line 1', 'address1', 'address line 1', 'address']
ADDRESS2_PATTERNS = ['donor address line 2', 'address2', 'address line 2']
CITY_PATTERNS = ['city', 'town', 'municipality']
ZIP_PATTERNS = ['zip', 'zip code', 'postal code', 'postal']
DONOR_STATUS_PATTERNS = ['donor status', 'status']
BIRTHDATE_PATTERNS = ['birthdate', 'birth date', 'date of birth', 'dob', 'birth day']

def find_column_by_pattern(columns, patterns):
    """Find the index of a column that best matches the given patterns."""
    # Try exact match first
    for pattern in patterns:
        for i, col in enumerate(columns):
            if str(col).lower() == pattern:
                return i
    
    # Then try contains match
    for pattern in patterns:
        for i, col in enumerate(columns):
            if pattern in str(col).lower():
                return i
    
    # Return first column as default if no match found
    return 0 if columns else None

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

def format_birthdate(birthdate):
    """Format birthdate to YYYY-MM-DD format for Google Sheets."""
    if pd.isna(birthdate):
        return None
    
    try:
        # Try to convert to datetime
        if isinstance(birthdate, str):
            # Try common date formats
            for fmt in ['%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d', '%Y/%m/%d', '%m-%d-%Y', '%d-%m-%Y']:
                try:
                    date_obj = datetime.strptime(birthdate, fmt)
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        
        # If it's already a datetime or date object
        if hasattr(birthdate, 'strftime'):
            return birthdate.strftime('%Y-%m-%d')
        
        # If all else fails, return as is
        return str(birthdate)
    except Exception:
        return str(birthdate)

def process_data(df, column_mapping):
    """Process the data based on column mapping."""
    # First, prepare the data using the provided column mapping
    # Convert 'Last Donation Date' to datetime for proper comparison if it exists
    if 'last_donation_date' in column_mapping and column_mapping['last_donation_date']:
        df['Last 	Donation Date'] = pd.to_datetime(df[column_mapping['last_donation_date']], errors='coerce')
    
    # Sort by 'Last Donation Date' in descending order and remove duplicates
    # keeping the first occurrence (which will be the most recent due to sorting)
    # Now considering both Donor # and Facility for duplicates
    if 'donor_number' in column_mapping and column_mapping['donor_number'] and 'facility' in column_mapping and column_mapping['facility'] and 'last_donation_date' in column_mapping and column_mapping['last_donation_date']:
        df = df.sort_values('Last 	Donation Date', ascending=False).drop_duplicates(subset=[column_mapping['donor_number'], column_mapping['facility']])
    
    # Create a new DataFrame with only required columns
    processed_df = pd.DataFrame()
    
    # Copy basic columns and preserve Donor # exactly as is
    if 'donor_number' in column_mapping and column_mapping['donor_number']:
        processed_df['Donor #'] = df[column_mapping['donor_number']]
    
    if 'donor_account' in column_mapping and column_mapping['donor_account']:
        processed_df['Donor Account #'] = df[column_mapping['donor_account']]
    
    if 'zip_code' in column_mapping and column_mapping['zip_code']:
        processed_df['Zip Code'] = df[column_mapping['zip_code']]
    
    if 'donor_status' in column_mapping and column_mapping['donor_status']:
        processed_df['Donor Status'] = df[column_mapping['donor_status']]
    
    if 'facility' in column_mapping and column_mapping['facility']:
        processed_df['Facility'] = df[column_mapping['facility']]
    
    # Process names
    if 'donor_name' in column_mapping and column_mapping['donor_name']:
        names = df[column_mapping['donor_name']].apply(process_name)
        processed_df['Donor First'] = names.apply(lambda x: x[0])
        processed_df['Donor Last'] = names.apply(lambda x: x[1])
    
    # Process email
    if 'donor_email' in column_mapping and column_mapping['donor_email']:
        processed_df['Donor E-mail'] = df[column_mapping['donor_email']].str.lower()
        # Remove both types of invalid emails
        invalid_emails = ['someone@plasmaworld.com', 'someone@plasma.com', 'some@plasmaworld.com','someone@plasmaworld.om', 'na@na.com', 'someoneinplasma@gmail.com', 'someoneinplasma@gmail.com']
        processed_df.loc[processed_df['Donor E-mail'].isin(invalid_emails), 'Donor E-mail'] = None
    
    # Process phone
    if 'donor_phone' in column_mapping and column_mapping['donor_phone']:
        processed_df['Donor Phone'] = df[column_mapping['donor_phone']].apply(format_phone)
    
    # Combine addresses
    if 'address_line1' in column_mapping and column_mapping['address_line1'] and 'address_line2' in column_mapping and column_mapping['address_line2']:
        processed_df['Donor Address'] = df[column_mapping['address_line1']].fillna('') + ' ' + df[column_mapping['address_line2']].fillna('')
        processed_df['Donor Address'] = processed_df['Donor Address'].str.strip()
    elif 'address_line1' in column_mapping and column_mapping['address_line1']:
        processed_df['Donor Address'] = df[column_mapping['address_line1']].fillna('')
    
    # Process birthdate
    if 'birthdate' in column_mapping and column_mapping['birthdate']:
        processed_df['Birthdate'] = df[column_mapping['birthdate']].apply(format_birthdate)

    # Reset index
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
        
        return True, "File structure is valid!", df
    
    except Exception as e:
        return False, f"Error reading file: {str(e)}", None

def render_column_mapping_ui(df):
    """Render UI for column mapping."""
    st.markdown("### Map Columns")
    
    # Add helpful instructions
    with st.expander("Instructions for Column Mapping", expanded=True):
        st.markdown("""
        **How to Map Columns:**
        
        1. Select the appropriate columns from your data for each required field.
        2. If a field doesn't exist in your data, leave it as is.
        3. The birthdate field is new and will be stored in the Google Sheet.
        4. After mapping columns, click "Process Data" to continue.
        """)
    
    # Get column names from dataframe
    columns = df.columns.tolist()
    
    # Find default column indices
    donor_name_idx = find_column_by_pattern(columns, DONOR_NAME_PATTERNS)
    donor_email_idx = find_column_by_pattern(columns, DONOR_EMAIL_PATTERNS)
    donor_account_idx = find_column_by_pattern(columns, DONOR_ACCOUNT_PATTERNS)
    donor_phone_idx = find_column_by_pattern(columns, DONOR_PHONE_PATTERNS)
    donor_number_idx = find_column_by_pattern(columns, DONOR_NUMBER_PATTERNS)
    facility_idx = find_column_by_pattern(columns, FACILITY_PATTERNS)
    address1_idx = find_column_by_pattern(columns, ADDRESS1_PATTERNS)
    address2_idx = find_column_by_pattern(columns, ADDRESS2_PATTERNS)
    city_idx = find_column_by_pattern(columns, CITY_PATTERNS)
    zip_idx = find_column_by_pattern(columns, ZIP_PATTERNS)
    donor_status_idx = find_column_by_pattern(columns, DONOR_STATUS_PATTERNS)
    birthdate_idx = find_column_by_pattern(columns, BIRTHDATE_PATTERNS)
    
    col1, col2, col3 = st.columns(3)
    
    column_mapping = {}
    
    with col1:
        column_mapping['donor_name'] = st.selectbox(
            "Donor Name Column", 
            options=[""] + columns,
            index=donor_name_idx + 1 if donor_name_idx is not None else 0
        )
        
        column_mapping['donor_email'] = st.selectbox(
            "Donor Email Column", 
            options=[""] + columns,
            index=donor_email_idx + 1 if donor_email_idx is not None else 0
        )
        
        column_mapping['donor_account'] = st.selectbox(
            "Donor Account Column", 
            options=[""] + columns,
            index=donor_account_idx + 1 if donor_account_idx is not None else 0
        )
        
        column_mapping['donor_phone'] = st.selectbox(
            "Donor Phone Column", 
            options=[""] + columns,
            index=donor_phone_idx + 1 if donor_phone_idx is not None else 0
        )
    
    with col2:
        column_mapping['donor_number'] = st.selectbox(
            "Donor # Column", 
            options=[""] + columns,
            index=donor_number_idx + 1 if donor_number_idx is not None else 0
        )
        
        column_mapping['facility'] = st.selectbox(
            "Facility Column", 
            options=[""] + columns,
            index=facility_idx + 1 if facility_idx is not None else 0
        )
        
        column_mapping['address_line1'] = st.selectbox(
            "Address Line 1 Column", 
            options=[""] + columns,
            index=address1_idx + 1 if address1_idx is not None else 0
        )
        
        column_mapping['address_line2'] = st.selectbox(
            "Address Line 2 Column", 
            options=[""] + columns,
            index=address2_idx + 1 if address2_idx is not None else 0
        )
    
    with col3:
        column_mapping['city'] = st.selectbox(
            "City Column", 
            options=[""] + columns,
            index=city_idx + 1 if city_idx is not None else 0
        )
        
        column_mapping['zip_code'] = st.selectbox(
            "Zip Code Column", 
            options=[""] + columns,
            index=zip_idx + 1 if zip_idx is not None else 0
        )
        
        column_mapping['donor_status'] = st.selectbox(
            "Donor Status Column", 
            options=[""] + columns,
            index=donor_status_idx + 1 if donor_status_idx is not None else 0
        )
        
        column_mapping['birthdate'] = st.selectbox(
            "Birthdate Column (New)", 
            options=[""] + columns,
            index=birthdate_idx + 1 if birthdate_idx is not None else 0
        )
    
    # Empty strings should be None
    for key in column_mapping:
        if column_mapping[key] == "":
            column_mapping[key] = None
    
    # Check for required mapping
    required_fields = ['donor_name', 'donor_number', 'facility']
    missing_required = [field for field in required_fields if not column_mapping.get(field)]
    
    if missing_required:
        st.warning(f"⚠️ The following required fields are not mapped: {', '.join(missing_required)}")
        st.info("Please select a column for each required field to continue.")
        can_process = False
    else:
        can_process = True
    
    return column_mapping, can_process

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
        headers = all_values[0][:11]  # Updated to include Birthdate
        data = [row[:11] for row in all_values[1:]]
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
                        'Zip Code', 'Donor Status', 'Center', 'Birthdate']
    
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
    fields_to_compare = ['Donor E-mail', 'Donor Phone', 'Donor Address', 'Center', 'Birthdate']
    for field in fields_to_compare:
        if f'{field}_new' in comparison_df.columns and f'{field}_master' in comparison_df.columns:
            comparison_df[f'{field}_new'] = comparison_df[f'{field}_new'].apply(standardize_value)
            comparison_df[f'{field}_master'] = comparison_df[f'{field}_master'].apply(standardize_value)
    
    # Check for changes in specific fields (ignoring format)
    updated_mask = (
        (comparison_df['Donor E-mail_new'] != comparison_df['Donor E-mail_master']) |
        (comparison_df['Donor Phone_new'] != comparison_df['Donor Phone_master']) |
        (comparison_df['Donor Address_new'] != comparison_df['Donor Address_master']) |
        (comparison_df['Center_new'] != comparison_df['Center_master']) |
        (comparison_df.get('Birthdate_new', '') != comparison_df.get('Birthdate_master', ''))
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
            (comparison_df['Center_new'] != comparison_df['Center_master']) |
            (comparison_df.get('Birthdate_new', '') != comparison_df.get('Birthdate_master', ''))
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
        'Donor Phone', 'Donor Address', 'Zip Code', 'Donor Status', 'Center', 'Birthdate'
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
        
        # Replace NaN values with empty strings
        df_clean = df.fillna('')
        
        # Update with new content
        worksheet.update([df_clean.columns.values.tolist()] + df_clean.values.tolist())
        return True
    except Exception as e:
        st.error(f"Error saving to Google Sheets: {str(e)}")
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
            'Donor Phone', 'Donor Address', 'Zip Code', 'Donor Status', 'Center', 'Birthdate'
        ]

        # Combine new and updated records
        records_to_append = pd.concat([new_donors, really_updated], ignore_index=True)
        if records_to_append.empty:
            return True

        # Rename Facility to Center if it exists
        if 'Facility' in records_to_append.columns:
            records_to_append['Center'] = records_to_append['Facility']
            records_to_append = records_to_append.drop('Facility', axis=1)

        # Reorder columns to match required order
        records_to_append = records_to_append[UPLOAD_COLUMNS]

        # Prepare records for upload (replace NaN with empty string)
        records_clean = records_to_append.fillna('')
        
        # Get the last row with data
        last_row = len(worksheet.get_all_values())
        
        # Append new records starting from the next row
        worksheet.append_rows(
            records_clean.values.tolist(),
            value_input_option='RAW',
            insert_data_option='INSERT_ROWS',
            table_range=f'A{last_row + 1}'
        )
        
        return True
    except Exception as e:
        st.error(f"Error appending to UPLOAD_PROCESS: {str(e)}")
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
        # Validate the file
        is_valid, message, df = validate_file(uploaded_file)
        
        if is_valid:
            # Show column mapping UI
            column_mapping, can_process = render_column_mapping_ui(df)
            
            if can_process and st.button("Process Data and Update Database"):
                with st.spinner("Processing data and updating databases..."):
                    # Store initial record count
                    initial_records = len(df)
                    
                    # Process the data with column mapping
                    processed_df = process_data(df, column_mapping)
                    
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
