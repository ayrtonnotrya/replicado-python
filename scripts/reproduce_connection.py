
import os
import pymssql
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("REPLICADO_HOST")
port = os.getenv("REPLICADO_PORT")
database = os.getenv("REPLICADO_DATABASE")
user = os.getenv("REPLICADO_USERNAME")
password = os.getenv("REPLICADO_PASSWORD")

print(f"Connecting to {host}:{port} as {user}...")

tds_versions = ['7.0', '7.1', '7.2', '7.3', '7.4']

for tds in tds_versions:
    print(f"\nTesting TDS Version: {tds}")
    try:
        conn = pymssql.connect(
            server=host,
            user=user,
            password=password,
            database=database,
            port=port,
            charset='utf8',
            tds_version=tds
        )
        print(f"✅ Connection successful with TDS {tds}!")
        conn.close()
        break
    except Exception as e:
        print(f"❌ Connection failed with TDS {tds}: {e}")
