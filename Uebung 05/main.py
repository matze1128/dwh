import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import numpy as np


# Lädt die Umgebungsvariablen aus der .env-Datei
load_dotenv()

# Zugriff auf die Umgebungsvariablen
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

# SQL-Abfrage mit einem Join zwischen fact_messung und dim_fahrzeug
# SQL-Abfrage zur Erstellung einer Pivot-Tabelle für die Heatmap
query = """
SELECT 
    k.land, 
    f.modell, 
    COUNT(ka.dim_kunde_id) AS anzahl_verkäufe, 
    AVG(ka.rabatt_pct) AS durchschn_rabatt,
    AVG(ka.kaufpreis) AS durchschn_kaufpreis
FROM 
    mart.dim_kunde k
JOIN 
    mart.fact_kauf ka ON k.dim_kunde_id = ka.dim_kunde_id
JOIN 
    mart.dim_fahrzeug f ON ka.dim_fahrzeug_id = f.dim_fahrzeug_id
GROUP BY 
    k.land, f.modell;
"""


# Verbindung zur Datenbank herstellen

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}/{db_name}')

# Verbindung zur Datenbank herstellen und SQL-Abfrage ausführen
try:
    df = pd.read_sql_query(query, engine)

    # Die einzigartigen Länder und Modelle aus den Daten extrahieren
    countries = df['land'].unique()
    models = df['modell'].unique()
    
    # Maximale Blasengröße festlegen
    max_bubble_size = 1000

    # Bubble-Chart erstellen
    plt.figure(figsize=(14,8))
    
    # Einzigartige Länder und Modelle für die Achsenbeschriftung
    countries = df['land'].unique()
    models = df['modell'].unique()
    
    # Erstellen eines Nummernindex für jedes Land und Modell
    country_idx = {country: idx for idx, country in enumerate(countries, start=1)}
    model_idx = {model: idx for idx, model in enumerate(models, start=1)}
    
    # Mapping der Länder und Modelle auf den Index
    df['land_idx'] = df['land'].map(country_idx)
    df['modell_idx'] = df['modell'].map(model_idx)

    # Die Größe der Blasen basierend auf der Anzahl der Verkäufe berechnen
    bubble_size = (df['anzahl_verkäufe'] / df['anzahl_verkäufe'].max()) * max_bubble_size

    # Erstellen der Scatter-Plots für jede Kategorie
    scatter = plt.scatter(
        df['land_idx'], df['modell_idx'], 
        s=bubble_size, 
        c=df['durchschn_kaufpreis'],  # Verwendung des durchschnittlichen Kaufpreises für die Farbe
        alpha=0.5, 
        cmap='viridis', 
        edgecolors='w', 
        linewidth=0.5
    )
    
    # Beschriftung der Blasen mit der Anzahl der Verkäufe
    for i in range(len(df)):
        plt.text(df['land_idx'][i], df['modell_idx'][i], 
                 f"{df['anzahl_verkäufe'][i]}", 
                 ha='center', va='center', 
                 color='black', fontsize=8)

    # Farblegende für den durchschnittlichen Kaufpreis hinzufügen
    plt.colorbar(scatter, label='Durchschnittlicher Kaufpreis (€)')

    # Achsen und Titel beschriften
    plt.title('Verkäufe und durchschnittlicher Kaufpreis nach Land und Fahrzeugmodell')
    plt.xlabel('Land')
    plt.ylabel('Fahrzeugmodell')
    plt.xticks(ticks=np.arange(1, len(countries) + 1), labels=countries, rotation=45)
    plt.yticks(ticks=np.arange(1, len(models) + 1), labels=models)
    plt.grid(True)
    plt.tight_layout()  # Stellt sicher, dass nichts abgeschnitten wird
    plt.show()

except Exception as e:
    print(f"Es gab einen Fehler beim Verbinden zur Datenbank: {e}")
