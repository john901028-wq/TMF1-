import pandas as pd

print("="*60)
print("Converting CSV to Excel")
print("="*60)

files = [
    'spy_clean.csv',
    'mxf_clean.csv',
    'mxf_spy_paired.csv'
]

for csv_file in files:
    try:
        df = pd.read_csv(csv_file)
        excel_file = csv_file.replace('.csv', '.xlsx')
        df.to_excel(excel_file, index=False, sheet_name='Data')
        print(f"OK - {csv_file} -> {excel_file} ({len(df)} rows)")
    except Exception as e:
        print(f"Error - {csv_file}: {e}")

print("\n" + "="*60)
print("Conversion complete!")
print("="*60)
print("\nGenerated Excel files:")
print("  1. spy_clean.xlsx")
print("  2. mxf_clean.xlsx")
print("  3. mxf_spy_paired.xlsx")
print("="*60)
