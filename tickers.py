import streamlit as st
import pandas as pd
from nasdaq import (fetch_variances, make_request_with_retry, parse_response, extract_date_reported)
import concurrent.futures

@st.cache_data(ttl=3600) # El caché expira en 1 hora
def cached_make_request(url):
    return make_request_with_retry(url)

@st.cache_resource
def load_all_platforms():
    ticker_files = {
        "IQ Option": "IQ_Option_Tickers.txt",
        "Moneta": "Moneta_Tickers.txt",
        "XTrend Speed": "XTrend_Speed_Tickers.txt"
    }
    platform_data = {}
    for name, file_path in ticker_files.items():
        try:
            with open(file_path, "r") as f:
                # Usamos set para búsquedas ultrarrápidas
                platform_data[name] = set(line.strip() for line in f)
        except FileNotFoundError:
            platform_data[name] = set()
    return platform_data

PLATFORMS_CACHE = load_all_platforms()

def check_tickers(ticker):
    platforms = [
        name for name, tickers in PLATFORMS_CACHE.items() 
        if ticker in tickers
    ]
    return {"platforms": platforms}

def process_single_company(item):
    """Procesa una empresa individualmente para ser ejecutada en paralelo."""
    ticker_info = check_tickers(item['symbol'])
    
    if ticker_info and ticker_info['platforms']:
        item['platforms'] = ticker_info['platforms']
        
        # Petición de earnings-surprise
        url = f"https://api.nasdaq.com/api/company/{item['symbol']}/earnings-surprise"
        response = cached_make_request(url)
        
        if response:
            date_reported_list = extract_date_reported(parse_response(response))
            if date_reported_list:
                # fetch_variances ya usa hilos internamente, lo cual es genial
                variances, avg_variance = fetch_variances(item['time'], item['symbol'], date_reported_list)
                
                if variances and any(v > 5 for v in variances):
                    variances_str = f"{', '.join(map(str, variances))} ({avg_variance})"
                    return {**item, 'variances': variances_str}
    return None

# Function to display progress
def display_progress(companies_list):
    filtered_companies = []
    num_companies = len(companies_list)
    
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Usamos ThreadPoolExecutor para procesar múltiples empresas en paralelo
    # max_workers=10 es un buen balance para no saturar la API
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Mapeamos la función a la lista de empresas
        future_to_company = {executor.submit(process_single_company, c): c for c in companies_list}
        
        for idx, future in enumerate(concurrent.futures.as_completed(future_to_company), 1):
            result = future.result()
            if result:
                filtered_companies.append(result)
            
            # Actualizamos la UI
            progreso = idx / num_companies
            progress_bar.progress(progreso)
            status_text.text(f"Analizado {idx}/{num_companies} empresas...")

    progress_bar.empty()
    status_text.empty()
    return filtered_companies

# Function to display filtered companies
def display_filtered_companies(filtered_companies):
    filtered_companies.sort(key=lambda x: x['symbol'])

    st.write("### Informe de Balances:")
    
    # Initialize the table data
    table_data = {
        "Horario": [],
        "Empresa": [],
        "Promedio (%)": [],
        "Plataformas": []  # New column for platform icons
    }
    
    # Platform icons mapping
    platform_icons = {
        "IQ Option": '<img src="https://topforextradingbrokers.com/wp-content/img/2020/08/IQ-1.png" width="30" height="30" title="IQ Option" alt="IQ Option"/>',
        "Moneta": '<img src="https://i.vimeocdn.com/portrait/49171687_640x640" width="30" height="30" title="Moneta" alt="Moneta"/>',
        "XTrend Speed": '<img src="https://is3-ssl.mzstatic.com/image/thumb/Purple124/v4/0a/3b/4f/0a3b4f51-70b3-5233-ff89-34f5ec3c79ea/source/1200x1200bb.png" width="30" height="30" title="XTrend Speed" alt="XTrend Speed"/>'
    }

    for item in filtered_companies:
        time_emoji = '🌞' if item['time'] == 'time-pre-market' else '🌛'

        table_data["Horario"].append(time_emoji)
        table_data["Empresa"].append(item['symbol'])
        table_data["Promedio (%)"].append(item['variances'])
        
        # Check platforms for the current ticker
        platforms = item.get('platforms', [])
        
        # Create HTML for the platforms
        platform_html = ' '.join([platform_icons[platform] for platform in platforms if platform in platform_icons])
        table_data["Plataformas"].append(platform_html)

    # Create a DataFrame with the table data
    df = pd.DataFrame(table_data)

    # Display the DataFrame as a table with HTML for the icons
    st.markdown(df.to_html(escape=False), unsafe_allow_html=True)