import os
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from sqlalchemy import create_engine

# Connexion à la base de données MySQL en utilisant SQLAlchemy
def get_db_connection():
    engine = create_engine('mysql+mysqlconnector://root:123456@localhost:3307/Weather_DataWarehouse')
    return engine

# Récupérer les données depuis la base de données
def fetch_weather_data(engine):
    query = """
        SELECT s.STATION, s.NAME, s.LATITUDE, s.LONGITUDE, s.ELEVATION, 
               d.DATE, d.YEAR, d.MONTH, d.DAY, 
               wf.PRCP, wf.PRCP_ATTRIBUTES, 
               wf.TAVG, wf.TAVG_ATTRIBUTES, 
               wf.TMAX, wf.TMAX_ATTRIBUTES, 
               wf.TMIN, wf.TMIN_ATTRIBUTES,
               c.Country_Name
        FROM Weather_Fact wf
        JOIN Station s ON wf.STATION = s.STATION
        JOIN Date d ON wf.DATE = d.DATE
        JOIN Country c ON wf.Country_Code = c.Country_Code
    """
    df = pd.read_sql(query, engine)
    return df

# Récupérer les données
engine = get_db_connection()
df = fetch_weather_data(engine)

# Convertir la colonne de date en format datetime
df['DATE'] = pd.to_datetime(df['DATE'])

# Ajouter des colonnes pour année, saison, trimestre et mois
df['YEAR'] = df['DATE'].dt.year
df['MONTH'] = df['DATE'].dt.month
df['QUARTER'] = df['DATE'].dt.quarter
df['SEASON'] = df['DATE'].dt.month % 12 // 3 + 1
season_labels = {1: 'Hiver', 2: 'Printemps', 3: 'Été', 4: 'Automne'}
df['SEASON'] = df['SEASON'].map(season_labels)

# Création de l'application Dash
app = dash.Dash(__name__)

# Mise en page de l'application Dash
app.layout = html.Div([
    html.H1("L'évolution des données climatiques :"),
    
    # Filtres
    html.Label('Année :'),
    dcc.Dropdown(
        id='year-dropdown',
        options=[{'label': str(year), 'value': year} for year in df['YEAR'].unique()],
        value=df['YEAR'].min()
    ),
    
    html.Label('Saison :'),
    dcc.Dropdown(
        id='season-dropdown',
        options=[{'label': season, 'value': season} for season in df['SEASON'].unique()],
        value=list(df['SEASON'].unique())[0]
    ),
    
    html.Label('Trimestre :'),
    dcc.Dropdown(
        id='quarter-dropdown',
        options=[{'label': f'T{quarter}', 'value': quarter} for quarter in df['QUARTER'].unique()],
        value=df['QUARTER'].min()
    ),
    
    html.Label('Mois :'),
    dcc.Dropdown(
        id='month-dropdown',
        options=[{'label': month, 'value': month} for month in df['MONTH'].unique()],
        value=df['MONTH'].min()
    ),
    
    # Carte géographique
    dcc.Graph(id='map-graph'),
])

# Callback pour mettre à jour la carte géographique en fonction des filtres sélectionnés
@app.callback(
    Output('map-graph', 'figure'),
    [Input('year-dropdown', 'value'),
     Input('season-dropdown', 'value'),
     Input('quarter-dropdown', 'value'),
     Input('month-dropdown', 'value')]
)
def update_map(selected_year, selected_season, selected_quarter, selected_month):
    filtered_df = df[(df['YEAR'] == selected_year) & 
                     (df['SEASON'] == selected_season) & 
                     (df['QUARTER'] == selected_quarter) & 
                     (df['MONTH'] == selected_month)]
    
    fig = px.scatter_geo(filtered_df, lat='LATITUDE', lon='LONGITUDE', color='PRCP',
                         hover_name='NAME', projection='natural earth',
                         title='Précipitations par emplacement')
    return fig

# Exécution de l'application Dash
if __name__ == '__main__':
    app.run_server(debug=True)
