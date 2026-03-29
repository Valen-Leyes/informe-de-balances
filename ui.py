import streamlit as st
from datetime import datetime
from nasdaq import fetch_data, filter_rows, extract_companies
from tickers import display_progress, display_filtered_companies

@st.cache_data(show_spinner="Cargando datos históricos y calculando variaciones...")
def get_cached_analysis(selected_date, companies_list):
    # Esta es la función que procesa todo en paralelo (la que optimizamos antes)
    return display_progress(companies_list)

def main():
    st.title('Análisis de Empresas')
    selected_date = st.date_input("Elige una fecha", datetime.now())
    
    response_data = fetch_data(selected_date)

    if response_data:
        filtered_rows = filter_rows(response_data)
        companies_list = extract_companies(filtered_rows)
        
        # El caché recordará el resultado para esta 'selected_date' específica
        filtered_companies = get_cached_analysis(selected_date, companies_list)
        
        display_filtered_companies(filtered_companies)

if __name__ == "__main__":
    main()
