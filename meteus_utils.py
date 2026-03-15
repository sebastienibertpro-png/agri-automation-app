import streamlit as st
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import pandas as pd

class MeteusClient:
    def __init__(self):
        # Load from secrets
        self.api_id = st.secrets["meteus"]["api_id"]
        self.api_password = st.secrets["meteus"]["api_password"]
        self.base_url = "http://api.meteus.fr/api"
        self.auth = HTTPBasicAuth(self.api_id, self.api_password)

    def get_stations(self):
        """Returns the list of stations for the account."""
        try:
            response = requests.get(f"{self.base_url}/export/stations", auth=self.auth)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            st.error(f"Erreur lors de la récupération des stations : {e}")
            return []

    @st.cache_data(ttl=1800) # Cache for 30 minutes
    def get_weather_summary(_self, station_id):
        """
        Fetches current weather and rain totals for 24h, 3d, 7j.
        Returns a dict with the summary.
        """
        try:
            # We fetch history for the last 7 days to calculate everything
            today = datetime.now()
            start_date = (today - timedelta(days=7)).strftime("%m-%d-%Y")
            
            # GET api/export/history/get?id={id}&cols=T,U,RR,DATETIME&scale=hour&p=7d
            # Note: doc says p=1d, 3m, 1y. For 7 days, we use dates.
            params = {
                "id": station_id,
                "cols": "T,U,RR,DATETIME",
                "from": start_date,
                "scale": "hour",
                "type": "json"
            }
            
            response = requests.get(f"{_self.base_url}/export/history/get", auth=_self.auth, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                return None
            
            df = pd.DataFrame(data)
            if df.empty:
                return None
                
            # Convert DATETIME
            df['DATETIME'] = pd.to_datetime(df['DATETIME'])
            df = df.sort_values('DATETIME', ascending=False)
            
            # Current values (last record)
            current = df.iloc[0]
            
            # Totals
            now = datetime.now()
            rain_24h = df[df['DATETIME'] >= (now - timedelta(hours=24))]['RR'].sum()
            rain_3j = df[df['DATETIME'] >= (now - timedelta(days=3))]['RR'].sum()
            rain_7j = df['RR'].sum()
            
            return {
                "temp": current.get('T', 0),
                "hum": current.get('U', 0),
                "rain_24h": rain_24h,
                "rain_3j": rain_3j,
                "rain_7j": rain_7j,
                "last_update": current['DATETIME'].strftime("%H:%M")
            }
            
        except Exception as e:
            print(f"Meteus Error: {e}")
            return None

def display_meteo_module():
    """Renders the weather module in Streamlit."""
    client = MeteusClient()
    stations = client.get_stations()
    
    if not stations:
        return
        
    station_id = stations[0]['Id'] # Default to first station
    summary = client.get_weather_summary(station_id)
    
    if summary:
        st.markdown(f"### 🌡️ Météo Station : {stations[0]['Name']} (MàJ {summary['last_update']})")
        col1, col2, col3, col4, col5 = st.columns(5)
        
        col1.metric("Température", f"{summary['temp']:.1f} °C")
        col2.metric("Hygrométrie", f"{summary['hum']:.0f} %")
        col3.metric("Pluie 24h", f"{summary['rain_24h']:.1f} mm")
        col4.metric("Pluie 3 jours", f"{summary['rain_3j']:.1f} mm")
        col5.metric("Pluie 7 jours", f"{summary['rain_7j']:.1f} mm")
        st.divider()
    else:
        st.warning("Impossible de récupérer les données météo en direct.")
