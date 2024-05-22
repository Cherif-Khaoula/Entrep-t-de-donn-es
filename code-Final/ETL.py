import os
import pandas as pd
import numpy as np
import mysql.connector
import csv
#ETL
#Extraction
# Définir le chemin du dossier contenant les fichiers CSV originaux
dossier_parent = "C:/Users/PC/Desktop/khaoula/M1ISII/s2/Entrepôt/Projet/Weather Data"

# Définir le chemin du nouveau dossier où les fichiers CSV traités seront enregistrés
nouveau_dossier = "C:/Users/PC/Desktop/khaoula/M1ISII/s2/Entrepôt/Projet/New weather Data"

# Parcourir chaque dossier (Algérie, Tunisie, Maroc)
for dossier in os.listdir(dossier_parent):
    # Chemin complet du dossier actuel
    chemin_dossier = os.path.join(dossier_parent, dossier)
    # S'assurer que le chemin actuel est un dossier
    if os.path.isdir(chemin_dossier):
        # Créer le chemin complet du nouveau dossier pour ce pays
        nouveau_dossier_pays = os.path.join(nouveau_dossier, dossier)
        # Vérifier si le nouveau dossier pour ce pays existe, sinon le créer
        if not os.path.exists(nouveau_dossier_pays):
            os.makedirs(nouveau_dossier_pays)
        # Parcourir chaque fichier dans le dossier actuel
        for fichier in os.listdir(chemin_dossier):
            # Vérifier si le fichier est un fichier CSV
            if fichier.endswith(".csv"):
                # Chemin complet du fichier CSV
                chemin_fichier = os.path.join(chemin_dossier, fichier)
                # Lire le fichier CSV en tant que DataFrame
                # Spécifiez les types de données pour chaque colonne
                #Transformation
                dtypes = {
                    'STATION': str,
                    'NAME': str,
                    'LATITUDE': float,
                    'LONGITUDE': float,
                    'ELEVATION': float,
                    'DATE': str,
                    'PRCP': float,
                    'PRCP_ATTRIBUTES': str,
                    'TAVG': float,
                    'TAVG_ATTRIBUTES': str,
                    'TMAX': float,
                    'TMAX_ATTRIBUTES': str,
                    'TMIN': float,
                    'TMIN_ATTRIBUTES': str
                }
                
                # Lisez le fichier CSV en utilisant les types de données spécifiés
                df = pd.read_csv(chemin_fichier, dtype=dtypes)
                # Extraire le nom du pays de la colonne "NAME"
                df['Country_Name'] = df['NAME'].apply(lambda x: x.split(',')[-1].strip())
                
                # Diviser la colonne "DATE" en colonnes "Year", "Month" et "Day"
                df['Year'] = pd.to_datetime(df['DATE']).dt.year
                df['Month'] = pd.to_datetime(df['DATE']).dt.month
                df['Day'] = pd.to_datetime(df['DATE']).dt.day
                
                # Supprimer la colonne "DATE"
                df.drop(columns=['DATE'], inplace=True)
                
                # Réorganiser les colonnes
                colonnes = ['STATION', 'NAME','LATITUDE', 'LONGITUDE', 'ELEVATION',  'Year', 'Month', 'Day', 'PRCP', 'PRCP_ATTRIBUTES', 'TAVG', 'TAVG_ATTRIBUTES', 'TMAX', 'TMAX_ATTRIBUTES', 'TMIN', 'TMIN_ATTRIBUTES', 'Country_Name']
                df = df[colonnes]
                
                # Remplacer les valeurs vides par 'None'
                df.replace(['',' ','n/a','--','NA'], np.nan, inplace=True)
                # Garder uniquement les colonnes spécifiées
                colonnes_specifiees = ['STATION', 'NAME', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'DATE', 'PRCP', 'PRCP_ATTRIBUTES', 'TAVG', 'TAVG_ATTRIBUTES', 'TMAX', 'TMAX_ATTRIBUTES', 'TMIN', 'TMIN_ATTRIBUTES']
                df = df[colonnes_specifiees] if all(col in df.columns for col in colonnes_specifiees) else df
                # Appliquer le remplacement par 'None'
                df.fillna(0, inplace=True)
                # Créer le chemin complet pour le nouveau fichier dans le nouveau dossier
                nouveau_chemin_fichier = os.path.join(nouveau_dossier_pays, fichier)
                # Enregistrer le DataFrame dans un nouveau fichier CSV
                df.to_csv(nouveau_chemin_fichier, index=False)

print("Traitement terminé. Les fichiers CSV ont été enregistrés dans le nouveau dossier.")


#Load
# Connexion à la base de données MySQL
conn = mysql.connector.connect(
    host="localhost",
    port="3307",
    user="root",
    password="123456",
    database="Weather_DataWarehouse"
)

try:
    cursor = conn.cursor()

    # Ajout manuel des entrées pour les pays dans la table Country
    pays = {
        "AG": "Algeria",
        "MO": "Maroc",
        "TS": "Tunisia"
    }
    
    for code, nom in pays.items():
        cursor.execute("INSERT INTO Country (Country_Code, Country_Name) VALUES (%s, %s)", (code, nom))
        conn.commit()

    # Fonction pour charger les données d'un fichier CSV dans une table MySQL
    def charger_donnees(conn, chemin_fichier_csv):
        try:
            print("Chargement des données depuis le fichier CSV...")

            # Ouvrir le fichier CSV
            with open(chemin_fichier_csv, 'r', encoding='utf-8') as csvfile:
                csvreader = csv.reader(csvfile)
                next(csvreader)  # Ignorer l'en-tête
                for row in csvreader:
                    station = row[0]
                    name = row[1]
                    latitude = float(row[2])
                    longitude = float(row[3])
                    elevation = float(row[4])

                    # Insérer les données dans la table Station
                    try:
                        cursor.execute("INSERT IGNORE INTO Station (STATION, NAME, LATITUDE, LONGITUDE, ELEVATION) VALUES (%s, %s, %s, %s, %s)", (station, name, latitude, longitude, elevation))
                    except mysql.connector.Error as e:
                        print(f"Erreur lors de l'insertion des données dans la table Station : {e}")
                        # Rollback la transaction en cas d'erreur
                        conn.rollback()
                        return
                    cursor.execute("SELECT STATION FROM Station WHERE STATION = %s", (station,))
                    station_id = cursor.fetchone()[0]  # Récupérer la première station correspondante


                   
                    year = int(row[5])
                    month = int(row[6])
                    day = int(row[7])
                    date = f"{year}-{month:02d}-{day:02d}"
                    prcp = float(row[8])
                    prcp_attributes = row[9]
                    tavg = float(row[10])
                    tavg_attributes = row[11]
                    tmax = float(row[12])
                    tmax_attributes = row[13]
                    tmin = float(row[14])
                    tmin_attributes = row[15]
                    country_code = row[16]

                    # Convertir le code de pays en nom de pays
                    country_name = pays.get(country_code)
                    if not country_name:
                        print(f"Le code de pays {country_code} n'a pas été trouvé dans la table des pays.")
                        continue

                    # Convertir la date en format DATE
                    cursor.execute("INSERT INTO Date (DATE, YEAR, MONTH, DAY) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE DATE=DATE", (date, year, month, day))

                    # Insérer les données dans la table Weather_Fact
                    query = "INSERT INTO Weather_Fact (STATION, DATE, PRCP, PRCP_ATTRIBUTES, TAVG, TAVG_ATTRIBUTES, TMAX, TMAX_ATTRIBUTES, TMIN, TMIN_ATTRIBUTES, Country_Code) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(query, (station_id, date, prcp, prcp_attributes, tavg, tavg_attributes, tmax, tmax_attributes, tmin, tmin_attributes, country_code))

            conn.commit()
            print("Données chargées avec succès.")

        except Exception as e:
            print(f"Erreur lors du chargement des données: {e}")

 
    # Parcourir récursivement les sous-dossiers dans le dossier principal
    for dossier_parent, sous_dossiers, fichiers in os.walk(nouveau_dossier):
        for fichier in fichiers:
            if fichier.endswith(".csv"):
                chemin_fichier_csv = os.path.join(dossier_parent, fichier)
                charger_donnees(conn, chemin_fichier_csv)

except mysql.connector.Error as error:
    print(f"Erreur lors de la connexion à MySQL: {error}")

finally:
    if conn.is_connected():
        conn.close()
        print("Connexion MySQL fermée.")
