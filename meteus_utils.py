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

    @st.cache_data(ttl=1800) 
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

            # Helper to find a column case-insensitively
            def get_col_name(columns, target):
                for c in columns:
                    if str(c).upper() == target.upper(): return c
                return None

            # 1. Fetch LIVE data
            data_live = fetch({"id": station_id, "p": "1d", "scale": "hour", "type": "json"})
            if not data_live: return None
            
            df = pd.DataFrame.from_records(data_live)
            if df.empty: return None
            
            # Identify columns for LIVE
            dt_col = get_col_name(df.columns, 'DATETIME') or get_col_name(df.columns, 'DATE')
            if not dt_col: return None
            
            t_col = get_col_name(df.columns, 'T')
            u_col = get_col_name(df.columns, 'U')
            rr_col = get_col_name(df.columns, 'RR')
            
            # Prepare LIVE DataFrame
            df[dt_col] = pd.to_datetime(df[dt_col], errors='coerce')
            df = df.dropna(subset=[dt_col]).copy()
            
            if t_col:
                df[t_col] = pd.to_numeric(df[t_col], errors='coerce')
                df = df[df[t_col].notna()].copy()
                
            df = df.sort_values(dt_col, ascending=False)
            if df.empty: return None
            
            now = datetime.now()
            current_row = df.iloc[0]
            
            # Rain 24h
            def safe_sum(dataframe, time_col, rain_col, delta, ref_time):
                if not rain_col or rain_col not in dataframe.columns: return 0.0
                cutoff = ref_time - delta
                mask = dataframe[time_col] >= cutoff
                subset = pd.to_numeric(dataframe.loc[mask, rain_col], errors='coerce').fillna(0.0)
                return float(subset.sum())

            rain_24h = safe_sum(df, dt_col, rr_col, timedelta(hours=24), now)
            
            # 2. Fetch HISTORICAL for 3j and 7j
            params_hist = {"id": station_id, "p": "1y", "scale": "day", "type": "json"}
            data_hist = fetch(params_hist)
            
            rain_3j, rain_7j = 0.0, 0.0
            if data_hist:
                df_h = pd.DataFrame.from_records(data_hist)
                if not df_h.empty:
                    # Identify columns for HISTORICAL (might be different!)
                    dt_h_col = get_col_name(df_h.columns, 'DATETIME') or get_col_name(df_h.columns, 'DATE')
                    rr_h_col = get_col_name(df_h.columns, 'RR')
                    
                    if dt_h_col and rr_h_col:
                        df_h[dt_h_col] = pd.to_datetime(df_h[dt_h_col], errors='coerce')
                        df_h = df_h.dropna(subset=[dt_h_col]).copy()
                        
                        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                        
                        def get_daily_rain(days_back):
                            cutoff = today_start - timedelta(days=days_back-1)
                            mask = df_h[dt_h_col] >= cutoff
                            subset = pd.to_numeric(df_h.loc[mask, rr_h_col], errors='coerce').fillna(0.0)
                            return float(subset.sum())

                        rain_3j = get_daily_rain(3)
                        rain_7j = get_daily_rain(7)

            return {
                "temp": float(current_row.get(t_col, 0)) if t_col else 0.0,
                "hum": float(current_row.get(u_col, 0)) if u_col else 0.0,
                "rain_24h": rain_24h,
                "rain_3j": rain_3j,
                "rain_7j": rain_7j,
                "last_update": current_row[dt_col].strftime("%H:%M")
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
        st.markdown(f"**🌡️ Station {stations[0]['Name']}** (MàJ {summary['last_update']})")
        
        # Custom CSS for compact, responsive grid
        st.markdown("""
        <style>
            .meteo-grid {
                display: grid;
                grid-template-columns: repeat(5, 1fr);
                gap: 10px;
                text-align: center;
                background-color: #f0f2f6;
                padding: 10px;
                border-radius: 10px;
                margin-bottom: 10px;
            }
            .meteo-item {
                display: flex;
                flex-direction: column;
            }
            .meteo-label {
                font-size: 0.75rem;
                color: #555;
                margin-bottom: 2px;
                white-space: nowrap;
            }
            .meteo-value {
                font-size: 1.1rem;
                font-weight: bold;
                color: #1f77b4;
            }
            @media (max-width: 600px) {
                .meteo-grid {
                    grid-template-columns: repeat(3, 1fr);
                }
            }
        </style>
        """, unsafe_allow_html=True)
        
        # Render HTML grid
        st.markdown(f"""
        <div class="meteo-grid">
            <div class="meteo-item"><span class="meteo-label">Temp</span><span class="meteo-value">{summary['temp']:.1f}°</span></div>
            <div class="meteo-item"><span class="meteo-label">Hum</span><span class="meteo-value">{summary['hum']:.0f}%</span></div>
            <div class="meteo-item"><span class="meteo-label">Pluie 24h</span><span class="meteo-value">{summary['rain_24h']:.1f}</span></div>
            <div class="meteo-item"><span class="meteo-label">3 Jours</span><span class="meteo-value">{summary['rain_3j']:.1f}</span></div>
            <div class="meteo-item"><span class="meteo-label">7 Jours</span><span class="meteo-value">{summary['rain_7j']:.1f}</span></div>
        </div>
        """, unsafe_allow_html=True)
        
        st.divider()
    else:
        st.warning("Impossible de récupérer les données météo en direct.")
