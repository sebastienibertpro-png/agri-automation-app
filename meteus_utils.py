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
        # Try HTTPS first, then fallback to HTTP if needed
        self.proto = "https"
        self.base_url = f"{self.proto}://api.meteus.fr/api"
        self.auth = HTTPBasicAuth(self.api_id, self.api_password)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json'
        }

    @st.cache_data(ttl=86400) # Cache stations list for 24 hours
    def get_stations(_self):
        """Returns the list of stations for the account."""
        urls = [
            f"https://api.meteus.fr/api/export/stations",
            f"http://api.meteus.fr/api/export/stations"
        ]
        
        last_error = None
        for url in urls:
            try:
                response = requests.get(
                    url, 
                    auth=_self.auth, 
                    headers=_self.headers,
                    timeout=20
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                last_error = e
                continue # Try next URL (HTTP fallback)
        
        st.error(f"Erreur de connexion Météus (Timeout) : {last_error}")
        return []

    @st.cache_data(ttl=1800) # Cache for 30 minutes
    def get_weather_summary(_self, station_id):
        """
        Fetches current weather and rain totals for 24h, 3d, 7j.
        """
        try:
            # We fetch 7 days of history
            today = datetime.now()
            start_date = (today - timedelta(days=8)).strftime("%m-%d-%Y")
            
            params = {
                "id": station_id,
                "from": start_date,
                "scale": "hour",
                "type": "json"
            }
            
            # Try HTTPS then HTTP
            url_https = f"https://api.meteus.fr/api/export/history/get"
            url_http = f"http://api.meteus.fr/api/export/history/get"
            
            response = None
            try:
                response = requests.get(url_https, auth=_self.auth, params=params, headers=_self.headers, timeout=20)
                response.raise_for_status()
            except:
                response = requests.get(url_http, auth=_self.auth, params=params, headers=_self.headers, timeout=20)
                response.raise_for_status()
            
            data = response.json()
            
            # Robust extraction: some APIs wrap the list in a dict (e.g. {"data": [...]})
            if isinstance(data, dict):
                # Try to find the list inside the dict
                for key in data:
                    if isinstance(data[key], list):
                        data = data[key]
                        break
            
            if not isinstance(data, list) or not data:
                return None
            
            # Create DataFrame from records list explicitly
            df = pd.DataFrame.from_records(data)
            
            if df.empty:
                return None
            
            # Identify columns with case-insensitive search
            actual_cols = df.columns.tolist()
            def find_col(target):
                for c in actual_cols:
                    if str(c).upper() == target.upper():
                        return c
                return None

            dt_col = find_col('DATETIME') or find_col('DATE')
            if not dt_col:
                return None

            # Convert to datetime and sort
            df[dt_col] = pd.to_datetime(df[dt_col], errors='coerce')
            df = df.dropna(subset=[dt_col])
            
            # Filter out future records (some APIs return placeholders for the whole day)
            # Use a threshold: allow up to 1 hour in the future to account for slight clock drifts
            now_ref = datetime.now()
            # If the server is in UTC and data is in Local (e.g. +1h), we need to be careful.
            # Best approach: only keep records that have a non-null temperature or similar
            t_col = find_col('T')
            if t_col:
                df = df[df[t_col].notna()]
            
            df = df.sort_values(dt_col, ascending=False)
            
            if df.empty:
                return None

            # IMPORTANT: Use the station's last record time as the reference for "Now" 
            # to avoid timezone mismatches between API and Streamlit server.
            last_record_time = df[dt_col].iloc[0]
            
            u_col = find_col('U')
            rr_col = find_col('RR')
            
            # Helper to get value or 0
            def get_val(row, col):
                if not col: return 0
                val = row.get(col, 0)
                try: return float(val) if pd.notna(val) else 0
                except: return 0

            # Safe totals calculation based on last_record_time
            def get_rain_sum(since_delta):
                if not rr_col: return 0
                # We calculate from the last known record backwards
                start_time = last_record_time - since_delta
                mask = (df[dt_col] > start_time) & (df[dt_col] <= last_record_time)
                subset = df[mask][rr_col]
                return pd.to_numeric(subset, errors='coerce').fillna(0).sum()

            return {
                "temp": get_val(df.iloc[0], t_col),
                "hum": get_val(df.iloc[0], u_col),
                "rain_24h": get_rain_sum(timedelta(hours=24)),
                "rain_3j": get_rain_sum(timedelta(days=3)),
                "rain_7j": get_rain_sum(timedelta(days=7)),
                "last_update": last_record_time.strftime("%H:%M"),
                "raw_data": df.head(5) # For debug if needed below
            }
            
        except Exception as e:
            st.error(f"Erreur technique Météus : {e}")
            import traceback
            print(traceback.format_exc())
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
        
        with st.expander("🔍 Debug Météus (Derniers relevés)"):
            if "raw_data" in summary:
                st.dataframe(summary["raw_data"])
        
        st.divider()
    else:
        st.warning("Impossible de récupérer les données météo en direct.")
