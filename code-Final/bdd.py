import mysql.connector

print("Tentative de connexion à MySQL...")

try:
    # Établir une connexion à MySQL
    conn = mysql.connector.connect(
        host="localhost",
        port="3307",  # Utilisez le port 3307
        user="root",
        password="123456",
    )
    print("Connexion réussie à MySQL.")

    # Créer la base de données Weather_DataWarehouse si elle n'existe pas déjà
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS Weather_DataWarehouse")
    print("Base de données créée avec succès.")

    # Créer les tables pour chaque pays si elles n'existent pas déjà
    cursor.execute("USE Weather_DataWarehouse")  # Utiliser la base de données créée
    print("Utilisation de la base de données Weather_DataWarehouse.")
    


    # Créer les tables de dimension
    cursor.execute("""
       CREATE TABLE IF NOT EXISTS Country (
         Country_Code CHAR(2) PRIMARY KEY,
         Country_Name VARCHAR(255)
       );
    """)
    conn.commit()
    print("Table Country créée avec succès.")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Station (
          STATION VARCHAR(255) PRIMARY KEY,
          NAME VARCHAR(255),
          LATITUDE FLOAT,
          LONGITUDE FLOAT,
          ELEVATION FLOAT
        );

    """)
    conn.commit()
    print("Table Station créée avec succès.")

    cursor.execute("""
       CREATE TABLE IF NOT EXISTS Date (
         DATE DATE PRIMARY KEY,
         YEAR INT,
         MONTH INT,
         DAY INT
       );

    """)
    conn.commit()
    print("Table Date créée avec succès.")
   
    # Créer la table de faits pour les mesures principales
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Weather_Fact (
          ID INT AUTO_INCREMENT PRIMARY KEY,
          STATION VARCHAR(255),
          DATE DATE,
          PRCP FLOAT,
          PRCP_ATTRIBUTES VARCHAR(255),
          TAVG FLOAT,
          TAVG_ATTRIBUTES VARCHAR(255),
          TMAX FLOAT,
          TMAX_ATTRIBUTES VARCHAR(255),
          TMIN FLOAT,
          TMIN_ATTRIBUTES VARCHAR(255),
          Country_Code CHAR(2),
          FOREIGN KEY (STATION) REFERENCES Station(STATION),
          FOREIGN KEY (DATE) REFERENCES Date(DATE),
          FOREIGN KEY (Country_Code) REFERENCES Country(Country_Code)
        );

    """)
    conn.commit()
    print("Table Weather_Fact créée avec succès.")

except mysql.connector.Error as error:
    print(f"Erreur lors de la connexion à MySQL: {error}")

finally:
    if (conn.is_connected()):
        cursor.close()
        conn.close()
        print("Connexion MySQL fermée.")
