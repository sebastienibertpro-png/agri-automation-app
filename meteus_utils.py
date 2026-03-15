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
        # Use HTTPS to avoid RemoteDisconnected and add a standard User-Agent
        self.base_url = "https://api.meteus.fr/api"
        self.auth = HTTPBasicAuth(self.api_id, self.api_password)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_stations(self):
        """Returns the list of stations for the account."""
        try:
            response = requests.get(
                f"{self.base_url}/export/stations", 
                auth=self.auth, 
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            st.error(f"Erreur lors de la récupération des stations : {e}")
            return []

    @st.cache_data(ttl=1800) # Cache for 30 minutes
    def get_weather_summary(_self, station_id):
        """
        Fetches current weather and rain totals for 24h, 3d, 7j.
        """
        try:
            # We fetch 7 days of history
            today = datetime.now()
            start_date = (today - timedelta(days=8)).strftime("%m-%d-%Y") # 8 to be safe for 7 full days
            
            params = {
                "id": station_id,
                "from": start_date,
                "scale": "hour",
                "type": "json"
            }
            
            response = requests.get(
                f"{_self.base_url}/export/history/get", 
                auth=_self.auth, 
                params=params,
                headers=_self.headers,
                timeout=30
            )
            
            if response.status_code != 200:
                st.error(f"Erreur API Météus ({response.status_code})")
                return None
                
            data = response.json()
            if not data:
                return None
            
            df = pd.DataFrame(data)
            if df.empty:
                return None
            
            # Identify columns
            cols_map = {c.upper(): c for c in df.columns}
            dt_col = cols_map.get('DATETIME') or cols_map.get('DATE')
            
            if not dt_col:
                return None

            df[dt_col] = pd.to_datetime(df[dt_col])
            df = df.sort_values(dt_col, ascending=False)
            
            now = datetime.now()
            current = df.iloc[0]
            
            t_col = cols_map.get('T')
            u_col = cols_map.get('U')
            rr_col = cols_map.get('RR')
            
            # Totals
            rain_24h = df[df[dt_col] >= (now - timedelta(hours=24))][rr_col].sum() if rr_col else 0
            rain_3j = df[df[dt_col] >= (now - timedelta(days=3))][rr_col].sum() if rr_col else 0
            rain_7j = df[df[dt_col] >= (now - timedelta(days=7))][rr_col].sum() if rr_col else 0
            
            return {
                "temp": current.get(t_col, 0) if t_col else 0,
                "hum": current.get(u_col, 0) if u_col else 0,
                "rain_24h": rain_24h,
                "rain_3j": rain_3j,
                "rain_7j": rain_7j,
                "last_update": df[dt_col].iloc[0].strftime("%H:%M")
            }
            
        except Exception as e:
            st.error(f"Erreur technique Météus : {e}")
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
