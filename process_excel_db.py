import pandas as pd
import pyodbc
import logging
import sys
from datetime import datetime, date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'server': '192.168.1.41',
    'database': 'DBInforma',
    'username': 'sa',
    'password': 'Informa2025$$',
    'port': 1433,
    'encrypt': 'yes',
    'trust_cert': 'yes'
}

TABLE_NAME = 'Huancayo.Base'
EXCEL_PATH = r'c:\Users\ASUS\Documents\prompts\examples\call.xlsx'

TELEPHONE_FIELDS = [
    'TELEFONO_FIJO_TITULAR',
    'TELEFONO_TITULAR',
    'TELEFONO_REPRESENTANTE',
    'TELEFONO_CONYUGE',
    'TELEFONO_CODEUDOR',
    'TELEFONO_FIADOR',
    'TELEFONO_CONY_FIADOR'
]

def format_value(val):
    """Formats values for Excel: no time in dates, no scientific notation in large numbers."""
    if pd.isna(val):
        return None
    if isinstance(val, (datetime, date)):
        return val.strftime('%Y-%m-%d')
    if isinstance(val, float):
        if val.is_integer():
            return str(int(val))
        return "{:.2f}".format(val).rstrip('0').rstrip('.')
    return str(val).strip()

def get_db_connection():
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={DB_CONFIG['server']},{DB_CONFIG['port']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
            f"Encrypt={DB_CONFIG['encrypt']};"
            f"TrustServerCertificate={DB_CONFIG['trust_cert']};"
        )
        conn = pyodbc.connect(conn_str)
        logger.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def search_in_db(cursor, cuenta, documento, apenom):
    # Cascaded Search
    queries = [
        ("CUENTA_CREDITO", cuenta),
        ("NRO_DNI", documento),
        ("CLIENTE_PREMIUM", apenom)
    ]
    
    for field, value in queries:
        if pd.isna(value) or str(value).strip() == "":
            continue
            
        # Ensure searching value is handled as string if it looks like a large number
        search_val = format_value(value)
        
        query = f"SELECT {', '.join(TELEPHONE_FIELDS)} FROM {TABLE_NAME} WHERE CAST({field} AS VARCHAR) = ?"
        try:
            cursor.execute(query, (search_val,))
            result = cursor.fetchone()
            if result:
                logger.info(f"Found match for {field}='{search_val}'")
                # Format each returned value
                return {k: format_value(v) for k, v in zip(TELEPHONE_FIELDS, result)}
        except Exception as e:
            logger.error(f"Error executing query for {field}='{search_val}': {e}")
            
    return None

def main():
    logger.info("Starting Excel enrichment process with improved formatting...")
    
    # Load Excel
    try:
        # Read all as strings to prevent scientific notation on initial load
        df = pd.read_excel(EXCEL_PATH, dtype=str)
        logger.info(f"Loaded Excel file with {len(df)} rows.")
        
        # Clean up existing dates if they have timestamps
        if 'FECHA DE PAGO' in df.columns:
            df['FECHA DE PAGO'] = df['FECHA DE PAGO'].apply(lambda x: x.split(' ')[0] if isinstance(x, str) and ' ' in x else x)
            
    except Exception as e:
        logger.error(f"Error loading Excel file: {e}")
        return

    # Initialize new columns as object/string
    for field in TELEPHONE_FIELDS:
        if field not in df.columns:
            df[field] = None
        df[field] = df[field].astype(object)

    # Connect to DB
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    # Process rows
    updated_count = 0
    for index, row in df.iterrows():
        # Pass the values as they are, search_in_db will handle formatting
        result = search_in_db(cursor, row['CUENTA'], row['DOCUMENTO'], row['APENOM'])
        if result:
            for field, value in result.items():
                df.at[index, field] = value
            updated_count += 1
            if updated_count % 100 == 0:
                logger.info(f"Progress: {updated_count} matches found...")
    
    logger.info(f"Finished processing rows. Updated {updated_count} rows.")
    
    # Save Excel
    try:
        # Save ensuring all data is treated as strings to preserve formatting
        df.to_excel(EXCEL_PATH, index=False)
        logger.info(f"Saved enriched data back to '{EXCEL_PATH}'.")
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}")
    
    conn.close()
    logger.info("Database connection closed.")

if __name__ == "__main__":
    main()
