import pandas as pd
import os

EXCEL_PATH = r'c:\Users\ASUS\Documents\prompts\examples\call.xlsx'

if not os.path.exists(EXCEL_PATH):
    print(f"Error: {EXCEL_PATH} not found.")
    exit(1)

df = pd.read_excel(EXCEL_PATH)
print(f"Total rows: {len(df)}")
print(f"Columns: {df.columns.tolist()}")

fields = [
    'TELEFONO_FIJO_TITULAR',
    'TELEFONO_TITULAR',
    'TELEFONO_REPRESENTANTE',
    'TELEFONO_CONYUGE',
    'TELEFONO_CODEUDOR',
    'TELEFONO_FIADOR',
    'TELEFONO_CONY_FIADOR'
]

print("\nNon-null counts:")
print(df[fields].notnull().sum())

print("\nSample of populated telephone field:")
populated = df[df['TELEFONO_TITULAR'].notnull()]
if not populated.empty:
    print(populated[['CUENTA', 'TELEFONO_TITULAR']].head(5))
else:
    print("No rows populated with TELEFONO_TITULAR.")
