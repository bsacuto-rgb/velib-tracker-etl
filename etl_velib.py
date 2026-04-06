import os
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

def run_etl():
    # --- 1. CONFIGURATION ---
    print("--- Démarrage de l'ETL ---")
    load_dotenv()
    
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    
    # URL de l'API Velib
    API_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=100"

    try:
        # --- 2. EXTRACT ---
        print("Extraction des données depuis l'API...")
        response = requests.get(API_URL)
        response.raise_for_status() # Vérifie si l'appel a réussi
        data = response.json()
        df = pd.DataFrame(data['results'])

        # --- 3. TRANSFORM ---
        print("Transformation des données...")
        # On ne garde que ce qui nous intéresse
        columns_to_keep = ['stationcode', 'name', 'capacity', 'numbikesavailable', 'mechanical', 'ebike', 'duedate']
        df = df[columns_to_keep]
        
        # Conversion de la date
        df['duedate'] = pd.to_datetime(df['duedate'])

        # --- 4. LOAD ---
        print("Connexion à AWS et chargement...")
        db_url = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
        engine = create_engine(db_url)

        df.to_sql(
            name='velib_stations', 
            con=engine, 
            if_exists='replace', 
            index=False
        )
        print("✅ SUCCESS : Les données sont à jour sur AWS !")

    except Exception as e:
        print(f"❌ ERREUR : Le pipeline a échoué : {e}")

if __name__ == "__main__":
    run_etl()