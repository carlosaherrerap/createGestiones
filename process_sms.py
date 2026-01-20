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

# Configuration
DB_CONFIG = {
    'server': '192.168.1.41',
    'database': 'DBInforma',
    'username': 'sa',
    'password': 'Informa2025$$',
    'port': 1433,
    'encrypt': 'yes',
    'trust_cert': 'yes'
}

INPUT_EXCEL = r'c:\Users\ASUS\Documents\prompts\examples\sms.xlsx'
OUTPUT_EXCEL = r'c:\Users\ASUS\Documents\prompts\examples\sms_procesado.xlsx'

def format_value(val):
    """Formats values for Excel: no time in dates, no scientific notation in large numbers."""
    if pd.isna(val) or val is None:
        return ""
    if isinstance(val, (datetime, date)):
        return val.strftime('%Y-%m-%d')
    if isinstance(val, (float, int)):
        if isinstance(val, float) and val.is_integer():
            return str(int(val))
        if isinstance(val, float):
            return "{:f}".format(val).rstrip('0').rstrip('.')
        return str(val)
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
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def main():
    logger.info("Starting SMS data enrichment...")
    
    # Load Source Excel
    try:
        # Read as string to preserve number formatting (like phone numbers)
        df_source = pd.read_excel(INPUT_EXCEL, dtype=str)
        logger.info(f"Loaded source Excel with {len(df_source)} rows.")
    except Exception as e:
        logger.error(f"Error loading source Excel: {e}")
        return

    # Connect to DB
    conn = get_db_connection()
    if not conn:
        logger.error("Could not establish database connection. Stopping.")
        return
    
    cursor = conn.cursor()
    
    # Query for retrieval
    query = "SELECT OBSERVACIONES, CUENTA FROM Huancayo.GesstionDiaria WHERE CAST(TELEFONO AS VARCHAR) = ?"
    
    processed_rows = []
    
    # Column Order Requested:
    # ID, CAMPAÑA, NUMERO, MENSAJE, FECHA DE ENVIO, HORA DE ENVIO, OBSERVACIONES, CARTERA, NUMERO DE CREDITO (CUENTA)
    
    total_rows = len(df_source)
    for idx, row in df_source.iterrows():
        numero = row.get('NUMERO', '').strip()
        
        if not numero:
            # If no number, just copy the row as is with empty new fields
            new_row = [
                row.get('ID', ''), row.get('CAMPAÑA', ''), row.get('NUMERO', ''),
                row.get('MENSAJE', ''), row.get('FECHA DE ENVIO', ''), row.get('HORA DE ENVIO', ''),
                '', row.get('CARTERA', ''), ''
            ]
            processed_rows.append(new_row)
            continue

        try:
            cursor.execute(query, (numero,))
            db_results = cursor.fetchall()
            
            if db_results:
                for obs, cuenta in db_results:
                    new_row = [
                        row.get('ID', ''), row.get('CAMPAÑA', ''), row.get('NUMERO', ''),
                        row.get('MENSAJE', ''), row.get('FECHA DE ENVIO', ''), row.get('HORA DE ENVIO', ''),
                        format_value(obs), row.get('CARTERA', ''), format_value(cuenta)
                    ]
                    processed_rows.append(new_row)
            else:
                # No results found, keep original row with empty placeholders
                new_row = [
                    row.get('ID', ''), row.get('CAMPAÑA', ''), row.get('NUMERO', ''),
                    row.get('MENSAJE', ''), row.get('FECHA DE ENVIO', ''), row.get('HORA DE ENVIO', ''),
                    '', row.get('CARTERA', ''), ''
                ]
                processed_rows.append(new_row)
        except Exception as e:
            logger.error(f"Error querying number {numero}: {e}")
            # In case of error, still preserve original row
            new_row = [
                row.get('ID', ''), row.get('CAMPAÑA', ''), row.get('NUMERO', ''),
                row.get('MENSAJE', ''), row.get('FECHA DE ENVIO', ''), row.get('HORA DE ENVIO', ''),
                '', row.get('CARTERA', ''), ''
            ]
            processed_rows.append(new_row)

        if (idx + 1) % 100 == 0:
            logger.info(f"Processed {idx + 1}/{total_rows} rows...")

    # Define headers
    headers = [
        'ID', 'CAMPAÑA', 'NUMERO', 'MENSAJE', 'FECHA DE ENVIO', 'HORA DE ENVIO',
        'OBSERVACIONES', 'CARTERA', 'NUMERO DE CREDITO'
    ]
    
    df_result = pd.DataFrame(processed_rows, columns=headers)
    
    # Save to Excel
    try:
        df_result.to_excel(OUTPUT_EXCEL, index=False)
        logger.info(f"Successfully saved results to '{OUTPUT_EXCEL}'. Total rows: {len(df_result)}")
    except Exception as e:
        logger.error(f"Error saving output Excel: {e}")

    conn.close()
    logger.info("Process completed.")

if __name__ == "__main__":
    main()
