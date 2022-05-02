import numpy as np
import requests
import pandas as pd
import time

# Exemplot SNIRH
#https://snirh.apambiente.pt/snirh/_dadosbase/site/paraCSV/dados_csv.php?sites=111717548&pars=898486382&tmin=30/06/2000&tmax=18/04/2022&formato=csv

STATIONS = {
    "ALBUFEIRA DO ALQUEVA": {"idata": "24M/05", "url_id": 111717548},
    "ALBUFEIRA MONTE DA ROCHA": {"idata": "27H/03", "url_id": 774113516},
    "ALCARRACHE": {"idata": "24N/01", "url_id": 111717557},
    "ALCARRACHE-CONFLUÊNCIA": {"idata": "4N/03", "url_id": 438864094},
    "ALCAÇOVAS": {"idata": "23I/02H", "url_id": 111717566},
    "ALQUEVA-CAPTAÇÃO": {"idata": "24L/03", "url_id": 111717554},
    "QUINTA DAS LARANJEIRAS (INAG) (06O/08H": {"idata": "06O/08H", "url_id": 111717490}
}

PARAMETERS = {
    #"Oxigénio Dissolvido - Campo (Meio) (mg/l)": 898486382,
    "Condutividade (uS/cm) (uS/cm)": 100003231,
    "Temperatura da Água (Meio) (°C)": 898486250
}

PERIOD = {
    "tmin": "30/04/2008",
    "tmax": "18/06/2008"
}

# Request delay in seconds (to avoid problems)
request_delay = 3

def snirh_extract():

    # Get geolocations
    geoStatData = pd.read_csv('data/rede_Qualidadeautomatica.csv', index_col=0, squeeze=True, encoding = "ISO-8859-1").to_dict()

    station_names = list(STATIONS.keys())
    parameter_names = list(PARAMETERS.keys())
    tmin = PERIOD["tmin"]
    tmax = PERIOD["tmax"]
    exportFormat = "csv"

    # Define dataframe column headers
    DFcolumnHeaders = ['datetime',
                     'idata',
                     'station_name',
                     'station_lat',
                     'station_lon',
                     'param_val']

    # Query base
    url_base_snirh = "https://snirh.apambiente.pt/snirh/_dadosbase/site/paraCSV/dados_csv.php?"

    # Create dictionary of dataframes, per parameter
    snirh_data = {}

    for parmName in parameter_names:

        try:
            param_url_id = PARAMETERS[parmName]

            # Creating an empty Dataframe for each parameter
            df_parameter = pd.DataFrame([])

            # Loop over stations
            for statName in station_names:

                try:
                    # Station info
                    idata = STATIONS[statName]["idata"]
                    station_url_id = STATIONS[statName]["url_id"]
                    station_name = geoStatData['NOME'][idata]
                    station_lat = geoStatData['LATITUDE (ºN)'][idata]
                    station_lon = geoStatData['LONGITUDE (ºW)'][idata]

                    # Queries
                    query_station = f"sites={station_url_id}"
                    query_param = f"pars={param_url_id}"
                    query_tmin = f"tmin={tmin}"
                    query_tmax = f"tmax={tmax}"
                    query_formato = f"formato={exportFormat}"
                    # URL query
                    final_url_query = f"{url_base_snirh}{query_station}&{query_param}&{query_tmin}&{query_tmax}&{query_formato}"

                    # https://snirh.apambiente.pt/snirh/_dadosbase/site/paraCSV/dados_csv.php?sites=111717548&pars=100003231&tmin=04/08/2003&tmax=04/07/2022&formato=csv
                    # https://snirh.apambiente.pt/snirh/_dadosbase/site/paraCSV/dados_csv.php?sites=111717548&pars=100003231&tmin=30/06/2012&tmax=18/04/2022&formato=csv
                    # URL request
                    time.sleep(request_delay)  # Seconds
                    f = requests.get(final_url_query)

                    # Processing of data
                    data_raw = str.split(f.text, sep='\n')[4:len(f.text)-4]
                    data_raw = data_raw[3:len(data_raw)-4]
                    datetime = np.array([data_raw_i.split(',').pop(0) for data_raw_i in data_raw])
                    data = np.array([data_raw_i.split(',').pop(1) for data_raw_i in data_raw])

                    df_Station_entry = pd.DataFrame({DFcolumnHeaders[0]: datetime,
                                                    DFcolumnHeaders[1]: [idata for i in range(len(datetime))],
                                                    DFcolumnHeaders[2]: [statName for i in range(len(datetime))],
                                                    DFcolumnHeaders[3]: [station_lat for i in range(len(datetime))],
                                                    DFcolumnHeaders[4]: [station_lon for i in range(len(datetime))],
                                                    DFcolumnHeaders[5]: data})

                    # Append to master parameter dataframe

                    if not df_Station_entry.empty:
                        df_parameter = df_parameter.append(df_Station_entry, ignore_index=True)
                        print(f"-> (mode_1=SNIRH)(data_exists=TRUE) Data extracted for Station {idata} ({statName}) on Parameter {parmName}")
                    else:
                        print(f"-> (mode_1=SNIRH)(data_exists=FLASE) Data extracted for Station {idata} ({statName}) on Parameter {parmName}")

                except:
                    print(f"-> (mode_1=SNIRH) Problem with Station {idata} ({statName}) on Parameter {parmName}")

            snirh_data[parmName] = df_parameter

        except:
            print(f"-> (mode_1=SNIRH) Problem with Parameter {parmName} (general)")


    return snirh_data
