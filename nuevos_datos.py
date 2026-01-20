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

TABLE_NAME = 'huancayo.GesstionDiaria'
INPUT_EXCEL = r'c:\Users\ASUS\Documents\prompts\examples\datos_fuente.xlsx'
OUTPUT_EXCEL = r'c:\Users\ASUS\Documents\prompts\examples\nuevos_datos.xlsx'

# Mapping of Output Field -> Database Field
MAPPING = [
    ('CUENTA', 'CUENTA'),
    ('EMPRESA RECAUDADORA EXTERNA', 'EMPRESA_RECAUDADORA_EXTERNA'),
    ('TIPO CARTERA', 'TIPO_cARTERA'),
    ('AGENCIA', 'AGENCIA'),
    ('CONTACTO', 'CLIENTE'),
    ('SALDO CAPITAL', 'SALDO_CAPITAL'),
    ('FECHA DE GESTION', 'FECHA_GESTION'),
    ('ATRASO', 'ATRASO'),
    ('TIPO DE GESTION', 'TIPO_GESTION'),
    ('TIPO DE GESTION(IVR,SMS,CALL, ETC)', 'TIPO_GESTION'),
    ('GESTION EFECTIVA', 'GESTION_EFECTIVA'),
    ('TELEFONO', 'TELEFONO'),
    ('DATA', 'DATA'),
    ('REFERENCIA', 'REFERENCIA'),
    ('RESULTADO', 'RESULTADO'),
    ('OBSERVACIONES(Detalle de resultado)', 'OBSERVACIONES'),
    ('FECHA DE COMPROMISO', 'FECHA_COMPROMISO'),
    ('MONTO DE COMPROMISO(Si es que tiene compromiso)', 'MONTO_COMPROMISO'),
    ('CLASIFICACION DEL CREDITO', 'CLASIFICACION_DEL_CREDITO'),
    ('UBICABLE TELEFONICAMENTE(SI/NO)', 'UBICABLE_TELEFONICAMENTE'),
    ('REPROGRAMARA', 'REPROGRAMARA'),
    ('TELEFONO', 'TELEFONO_REPROGRAMARA')
]

def format_value(val):
    """Formats values for Excel: no time in dates, no scientific notation in large numbers."""
    if pd.isna(val) or val is None:
        return ""
    
    # Handle dates
    if isinstance(val, (datetime, date)):
        return val.strftime('%Y-%m-%d')
    
    # Handle floats (avoid .0 and scientific notation)
    if isinstance(val, (float, int)):
        if isinstance(val, float) and val.is_integer():
            return str(int(val))
        if isinstance(val, float):
            # Format to avoid scientific notation
            return "{:f}".format(val).rstrip('0').rstrip('.')
        return str(val)
    
    # Default to string
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

def main():
    logger.info("Starting production of nuevos_datos.xlsx...")
    
    # Load Source Excel
    try:
        # Read as string to avoid scientific notation on initial load
        df_source = pd.read_excel(INPUT_EXCEL, dtype=str)
        logger.info(f"Loaded source Excel with {len(df_source)} rows.")
    except Exception as e:
        logger.error(f"Error loading source Excel: {e}")
        return

    if 'CUENTA' not in df_source.columns:
        logger.error("Column 'CUENTA' not found in source Excel.")
        return

    # Connect to DB
    conn = get_db_connection()
    if not conn:
        logger.error("Could not establish database connection. Stopping script.")
        return
    
    cursor = conn.cursor()
    
    # Prepare result data
    rows = []
    
    # Get database fields to select
    db_fields = list(set([m[1] for m in MAPPING]))

    select_clause = ", ".join(db_fields)
    query = f"SELECT {select_clause} FROM {TABLE_NAME} WHERE CAST(CUENTA AS VARCHAR) = ?"

    processed_count = 0
    for index, row in df_source.iterrows():
        cuenta = format_value(row['CUENTA'])
        if not cuenta:
            continue
            
        try:
            cursor.execute(query, (cuenta,))
            db_rows = cursor.fetchall()  # Fetch all results for this account
            
            if db_rows:
                for db_row in db_rows:
                    # Map results to a dict for easy lookup within this iteration
                    db_data = dict(zip(db_fields, db_row))
                    
                    # Construct output row as a LIST to avoid dict key collision
                    output_values = []
                    for out_name, db_name in MAPPING:
                        val = db_data.get(db_name)
                        output_values.append(format_value(val))
                    
                    rows.append(output_values)
            else:
                logger.warning(f"Account {cuenta} not found in database.")
                # Add one empty row for this account if not found
                empty_row = [""] * len(MAPPING)
                # Find index of CUENTA in mapping to set it
                for i, (out_name, db_name) in enumerate(MAPPING):
                    if out_name == 'CUENTA':
                        empty_row[i] = cuenta
                rows.append(empty_row)

        except Exception as e:
            logger.error(f"Error processing account {cuenta}: {e}")
            
        processed_count += 1
        if processed_count % 100 == 0:
            logger.info(f"Processed {processed_count} rows...")

    # Create result DataFrame using lists
    headers = [m[0] for m in MAPPING]
    df_result = pd.DataFrame(rows, columns=headers)
    
    # Save to Excel
    try:
        # Convert all to string to be sure
        df_result = df_result.astype(str)
        # Handle empty strings appearing as 'nan' or similar
        df_result.replace('nan', '', inplace=True)
        df_result.replace('None', '', inplace=True)
        
        df_result.to_excel(OUTPUT_EXCEL, index=False)
        logger.info(f"Successfully saved {len(df_result)} rows to '{OUTPUT_EXCEL}'.")
    except Exception as e:
        logger.error(f"Error saving output Excel: {e}")

    conn.close()
    logger.info("Process completed.")

    # Save to Excel
    try:
        # Convert all to string one last time to be sure
        df_result = df_result.astype(str)
        # Handle empty strings appearing as 'nan' or similar
        df_result.replace('nan', '', inplace=True)
        df_result.replace('None', '', inplace=True)
        
        df_result.to_excel(OUTPUT_EXCEL, index=False)
        logger.info(f"Successfully saved {len(df_result)} rows to '{OUTPUT_EXCEL}'.")
    except Exception as e:
        logger.error(f"Error saving output Excel: {e}")

    conn.close()
    logger.info("Process completed.")

if __name__ == "__main__":
    main()
