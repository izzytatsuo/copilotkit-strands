"""Run forecast_setup notebook via DuckDBETL.run_notebook."""
import sys
sys.path.insert(0, '.')

from tools.duckdb_etl import DuckDBETL

etl = DuckDBETL(enable_s3=True, debug=True)

result = etl.run_notebook(
    notebook_path='forecast_setup.ipynb',
    variables={
        'site_list_path': 'C:/Users/admsia/Downloads/Site List -  - 2_9_2026 - 09_44 AM.xlsx',
        'ct_file_path': 'C:/Users/admsia/Downloads/1770651797.csv',
    }
)

import json
print(json.dumps(result, indent=2, default=str))
