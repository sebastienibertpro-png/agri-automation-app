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

    @st.cache_data(ttl=300) # Cache for 5 minutes during debug
    def get_weather_summary(_self, station_id):
        """
        Fetches current weather and rain totals.
        """
        try:
            # Simple helper for requests with HTTP fallback
            def fetch(params):
                for proto in ["https", "http"]:
                    try:
                        url = f"{proto}://api.meteus.fr/api/export/history/get"
                        resp = requests.get(url, auth=_self.auth, params=params, headers=_self.headers, timeout=15)
                        if resp.status_code == 200:
                            raw_json = resp.json()
                            # Handle wrapping dicts like {"data": [...]}
                            if isinstance(raw_json, dict):
                                for v in raw_json.values():
                                    if isinstance(v, list): return v
                                return [raw_json] # Wrap single dict in list
                            return raw_json
                    except: continue
                return None

            # 1. Fetch LIVE data
            data_live = fetch({"id": station_id, "p": "1d", "scale": "hour", "type": "json"})
            if not data_live: return None
            
            df = pd.DataFrame.from_records(data_live)
            if df.empty: return None
            
            # Identify columns
            actual_cols = df.columns.tolist()
            def find_col(target):
                for c in actual_cols:
                    if str(c).upper() == target.upper(): return c
                return None

            dt_col = find_col('DATETIME') or find_col('DATE')
            t_col = find_col('T')
            u_col = find_col('U')
            rr_col = find_col('RR')
            
            if not dt_col: return None
            
            # Prepare DataFrame
            df[dt_col] = pd.to_datetime(df[dt_col], errors='coerce')
            df = df.dropna(subset=[dt_col]).copy() # Use copy() to avoid view issues
            
            if t_col:
                # Ensure T is numeric before filtering
                df[t_col] = pd.to_numeric(df[t_col], errors='coerce')
                df = df[df[t_col].notna()].copy()
                
            df = df.sort_values(dt_col, ascending=False)
            if df.empty: return None
            
            now = datetime.now()
            current_row = df.iloc[0]
            
            # Rain 24h
            def safe_sum(dataframe, time_col, rain_col, delta):
                if not rain_col: return 0.0
                cutoff = now - delta
                mask = dataframe[time_col] >= cutoff
                # explicit conversion to avoid the "Mixing dicts" warning/error
                subset = pd.to_numeric(dataframe.loc[mask, rain_col], errors='coerce').fillna(0.0)
                return float(subset.sum())

            rain_24h = safe_sum(df, dt_col, rr_col, timedelta(hours=24))
            
            # 2. Fetch HISTORICAL for 3j and 7j
            # Use scale='day' for historical to be sure we get full daily totals reliably
            params_hist = {
                "id": station_id,
                "p": "1y", # Large period to ensure we have everything
                "scale": "day",
                "type": "json"
            }
            data_hist = fetch(params_hist)
            
            rain_3j, rain_7j = 0.0, 0.0
            if data_hist:
                df_h = pd.DataFrame.from_records(data_hist)
                if not df_h.empty:
                    # Clean historical dataframe
                    dt_h_col = find_col('DATETIME') or find_col('DATE')
                    if dt_h_col:
                        df_h[dt_h_col] = pd.to_datetime(df_h[dt_h_col], errors='coerce')
                        df_h = df_h.dropna(subset=[dt_h_col]).copy()
                        
                        # Calculate rain sums relative to the last record's day or 'now'
                        # To be consistent with Météus app, we use 'today' as base
                        # Note: In 'day' scale, DATETIME is often just the date
                        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                        
                        def get_daily_rain(days_back):
                            cutoff = today_start - timedelta(days=days_back-1) # Current day + X previous
                            mask = df_h[dt_h_col] >= cutoff
                            subset = pd.to_numeric(df_h.loc[mask, rr_col], errors='coerce').fillna(0.0)
                            return float(subset.sum())

                        rain_3j = get_daily_rain(3)
                        rain_7j = get_daily_rain(7)

            return {
                "temp": float(current_row.get(t_col, 0)) if t_col else 0.0,
                "hum": float(current_row.get(u_col, 0)) if u_col else 0.0,
                "rain_24h": rain_24h,
                "rain_3j": rain_3j,
                "rain_7j": rain_7j,
                "last_update": current_row[dt_col].strftime("%H:%M"),
                "raw_data": df.head(10)
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
