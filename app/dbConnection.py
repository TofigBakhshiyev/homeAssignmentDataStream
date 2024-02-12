from dotenv import load_dotenv
import os
from pathlib import Path

dotenv_path = Path(__file__).resolve().parent / '.env'

try:
    load_dotenv(dotenv_path=dotenv_path)
except Exception as e:
    print("Error loading .env file:", e)

USER = os.getenv('USER')
PASS = os.getenv('PASSWORD')

# JDBC options
def connect():
    jdbc_options = {
        "url": "jdbc:postgresql://localhost:5432/postgres",
        "user": USER,
        "password": PASS,
        "driver": "org.postgresql.Driver"
    }
    return jdbc_options