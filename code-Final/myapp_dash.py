import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

# Connexion à la base de données MySQL
conn = mysql.connector.connect(
    host="localhost",
    port="3307",
    user="root",
    password="123456",
    database="Weather_DataWarehouse"
)

# Créer un moteur de base de données SQLAlchemy à partir de la connexion
engine = create_engine('mysql+mysqlconnector://root:123456@localhost:3307/Weather_DataWarehouse')

# Charger les données de la base de données dans un DataFrame Pandas
query = """
    SELECT DATE, PRCP, TAVG, TMAX, TMIN
    FROM Weather_Fact
"""
df = pd.read_sql_query(query, engine)

# Vérifier et convertir si nécessaire la colonne "DATE" en type datetime
df['DATE'] = pd.to_datetime(df['DATE'])

# Fermer la connexion à la base de données
conn.close()

# Création de l'application Dash
app = dash.Dash(__name__)

# Mise en page du tableau de bord
app.layout = html.Div([
    html.H1("Tableau de Bord Météorologique"),

    html.Div([
        html.Label("Choisir une année :"),
        dcc.Dropdown(
            id="dropdown-year",
            options=[{"label": str(year), "value": year} for year in df["DATE"].dt.year.unique()],
            value=df["DATE"].dt.year.max()
        )
    ]),

    html.Div([
        dcc.Graph(id="graph-precipitation")
    ]),

    html.Div([
        dcc.Graph(id="graph-temperature")
    ])
])

# Définition des callbacks pour les mises à jour dynamiques des graphiques
@app.callback(
    Output("graph-precipitation", "figure"),
    Output("graph-temperature", "figure"),
    Input("dropdown-year", "value")
)
def update_graphs(selected_year):
    filtered_df = df[df["DATE"].dt.year == selected_year]

    # Graphique de précipitation
    fig_precipitation = px.line(filtered_df, x="DATE", y="PRCP", title="Précipitation au cours de l'année")
    
    # Graphique de température
    fig_temperature = px.line(filtered_df, x="DATE", y=["TAVG", "TMAX", "TMIN"], title="Température au cours de l'année")

    return fig_precipitation, fig_temperature

# Exécution de l'application
if __name__ == '__main__':
    app.run_server(debug=True)
