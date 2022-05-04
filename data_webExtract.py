
import numpy as np
import requests
import pandas as pd
import time
import shutil
import os

# Exemplot SNIRH
#https://snirh.apambiente.pt/snirh/_dadosbase/site/paraCSV/dados_csv.php?sites=111717548&pars=898486382&tmin=30/06/2000&tmax=18/04/2022&formato=csv

STATIONS = {
    "ALBUFEIRA DO ALQUEVA": {"idata": "24M/05", "url_id": 111717548},
    "ALBUFEIRA MONTE DA ROCHA": {"idata": "27H/03", "url_id": 774113516},
    "ALCARRACHE": {"idata": "24N/01", "url_id": 111717557},
    "ALCARRACHE-CONFLUêNCIA": {"idata": "24N/03", "url_id": 438864094},
    "ALCAçOVAS": {"idata": "23I/02H", "url_id": 111717566},
    "ALGE": {"idata": "14H/02H", "url_id": 111717525},
    #"ALQUEVA JUSANTE (EDIA)": {"idata": "24M/06", "url_id": XXXXXXXXXXXXXX},
    "ALQUEVA-CAPTAçO M": {"idata": "24L/03", "url_id": 111717554},
    "ALQUEVA-GODELIM2": {"idata": "24N/04", "url_id": 716980882},
    "ALQUEVA-MOURÃO": {"idata": "23M/03", "url_id": 438860414},
    "AMARANTE (INAG)": {"idata": "06I/05H", "url_id": 111717499},
    "ARDILA": {"idata": "24O/01H", "url_id": 111717559},
    #"ARDILA CONFLUêNCIA (EDIA)": {"idata": "25M/01", "url_id": XXXXXXXXXXXXXX},
    #"AçUDE DE PEDROGÃO": {"idata": "25L/02H", "url_id": XXXXXXXXXXXXXX},
    "BADOCA": {"idata": "25E/02H", "url_id": 111717568},
    #"BARCA DE ALVA": {"idata": "07P/04", "url_id": XXXXXXXXXXXXXX},
    #"BARROSELAS": {"idata": "04E/07", "url_id": XXXXXXXXXXXXXX},
    "BEIRÃ": {"idata": "17N/01H", "url_id": 111717535},
    "BENSAFRIM": {"idata": "31E/01H", "url_id": 111717577},
    "BERTIANDOS": {"idata": "03F/03", "url_id": 111717479},
    "BODEGA": {"idata": "31K/03H", "url_id": 111717569},
    #"BOUçAS": {"idata": "01F/05", "url_id": XXXXXXXXXXXXXX},
    "CAIA-GUADIANA": {"idata": "20O/03H", "url_id": 111717552},
    #"CAIS DE SACAVEM": {"idata": "21C/05", "url_id": XXXXXXXXXXXXXX},
    "CAPTAçÃO DO RIO ÕNSUA": {"idata": "08G/02", "url_id": 111717510},
    "CASAIS": {"idata": "01H/03H", "url_id": 111717473},
    "CASTANHEIRO (INAG)": {"idata": "06M/05H", "url_id": 111717494},
    "CHIQUEDA": {"idata": "16D/03", "url_id": 111717522},
    "COIRO DA BURRA": {"idata": "31J/01H", "url_id": 111717570},
    "ERMIDA CORGO": {"idata": "06K/05H", "url_id": 111717497},
    "FLOR DA ROSA": {"idata": "23I/01H", "url_id": 111717565},
    #"FONTE VELHA": {"idata": "03G/06", "url_id": XXXXXXXXXXXXXX},
    "FONTES": {"idata": "15E/06", "url_id": 111717517},
    "FOZ DO RIBEIRO": {"idata": "30H/04", "url_id": 111717573},
    "FOZ DO SOUSA": {"idata": "07F/03H", "url_id": 111717501},
    #"FOZ DO TÂMEGA": {"idata": "07H/06", "url_id": XXXXXXXXXXXXXX},
    "FRAGAS DA TORRE (INAG)": {"idata": "08H/04H", "url_id": 111717505},
    "FRATEL": {"idata": "16K/06QA", "url_id": 8516505846},
    "FRONTEIRA": {"idata": "19L/01", "url_id": 111717543},
    "FÁBRICA DA MATRENA": {"idata": "16G/01H", "url_id": 111717526},
    "GEVIM": {"idata": "13H/04H", "url_id": 111717516},
    "GODELIM": {"idata": "24N/02", "url_id": 111717558},
    "GOLÃES": {"idata": "05H/04", "url_id": 111717558},
    "GUADIANA/TÁLIGA": {"idata": "22N/02", "url_id": 111717546},
    "HOMEM (FISCAL)": {"idata": "04G/10", "url_id": 111717483},
    #"LUCEFÉCIT (EDIA)": {"idata": "22N/03", "url_id": XXXXXXXXXXXXXX},
    #"MARACHÃO": {"idata": "04E/08", "url_id": XXXXXXXXXXXXXX},
    "MODELOS": {"idata": "06G/07", "url_id": 111717502},
    "MOINHO DA GAMITINHA": {"idata": "25G/03H", "url_id": 111717562},
    "MOINHO NOVO": {"idata": "18I/01H", "url_id": 111717541},
    "MONTE DA PONTE": {"idata": "27J/01H", "url_id": 111717555},
    "MONTE DA VINHA": {"idata": "21O/01H", "url_id": 111717545},
    "MONTE PISÃO": {"idata": "19N/01H", "url_id": 111717551},
    "M⁄RTEGA": {"idata": "25P/02H", "url_id": 111717560},
    "NABOS": {"idata": "26H/01H", "url_id": 111717564},
    "NOSSA SENHORA DA GRAçA": {"idata": "16K/05H", "url_id": 111717536},
    "ODEÁXERE": {"idata": "30E/05H", "url_id": 111717576},
    "OEIRAS": {"idata": "28K/02H", "url_id": 111717556},
    "OUTEIRO": {"idata": "02E/01", "url_id": 111717475},
    "PAREDES VITÓRIA": {"idata": "15D/02", "url_id": 111717521},
    "PAVIA": {"idata": "20I/04H", "url_id": 111717544},
    "PERAIS": {"idata": "16L/06QA", "url_id": 8516505842},
    #"PONTE ALENQUER": {"idata": "19C/03", "url_id": XXXXXXXXXXXXXX},
    "PONTE ALVALADE/CAMPILHAS": {"idata": "26G/04H", "url_id": 111717567},
    "PONTE ALVALADE/SADO": {"idata": "26G/05H", "url_id": 111717561},
    "PONTE ALVIELA (EN365)": {"idata": "18F/04", "url_id": 111717529},
    "PONTE BICO (CAVADO)": {"idata": "04G/07H", "url_id": 111717481},
    "PONTE CADAFAIS": {"idata": "19D/06H", "url_id": 1696212258},
    #"PONTE CANHA": {"idata": "21F/01", "url_id": XXXXXXXXXXXXXX},
    #"PONTE COURAçA": {"idata": "19D/05", "url_id": XXXXXXXXXXXXXX},
    #"PONTE COUçO (MOIMENTA)": {"idata": "02P/02H", "url_id": XXXXXXXXXXXXXX},
    "PONTE DE CORUCHE": {"idata": "20F/02H", "url_id": 111717537},
    #"PONTE ERRA": {"idata": "20G/01", "url_id": XXXXXXXXXXXXXX},
    "PONTE MESQUITA": {"idata": "30G/08H", "url_id": 111717572},
    #"PONTE MINHOTEIRA": {"idata": "09F/01", "url_id": XXXXXXXXXXXXXX},
    "PONTE MUCELA": {"idata": "12H/03H", "url_id": 111717515},
    "PONTE OTA": {"idata": "19D/04H", "url_id": 111717531},
    "PONTE PEDRINHA": {"idata": "12M/02H", "url_id": 111717524},
    "PONTE PEREIRO": {"idata": "30E/04H", "url_id": 111717578},
    "PONTE RABAL/OLEIRINHOS": {"idata": "02Q/01H", "url_id": 111717489},
    "PONTE RIBEIRA DE PERNES": {"idata": "17F/03H", "url_id": 111717528},
    "PONTE RIO MAçAS": {"idata": "04R/03", "url_id": 111717491},
    "PONTE RODOVIÁRIA": {"idata": "31H/02H", "url_id": 111717571},
    "PONTE SANTA CLARA": {"idata": "10K/01H", "url_id": 111717512},
    #"PONTE SANTO ESTEVÃO": {"idata": "20E/02", "url_id": XXXXXXXXXXXXXX},
    "PONTE SÃO JOÃO LOURE": {"idata": "10F/04", "url_id": 111717506},
    #"PONTE TROFA": {"idata": "05F/03", "url_id": XXXXXXXXXXXXXX},
    #"PONTE VALE MAIOR": {"idata": "09G/01", "url_id": XXXXXXXXXXXXXX},
    "PONTE VALE TELHAS": {"idata": "04N/01H", "url_id": 111717495},
    "PONTE VILA FORMOSA": {"idata": "18K/01H", "url_id": 111717542},
    "PONTE ÁGUEDA": {"idata": "10G/02H", "url_id": 111717508},
    "QUINTA DA BROA (NORTE)": {"idata": "17F/10", "url_id": 111717527},
    "QUINTA DAS LARANJEIRAS (INAG)": {"idata": "06O/08H", "url_id": 111717490},
    "QUINTA MARAVILHA": {"idata": "04N/06", "url_id": 111717493},
    "RIBEIRA DE CARVALHO/JUSANTE ETAR": {"idata": "17E/07", "url_id": 111717530},
    "RIO ARNÓIA": {"idata": "17B/02BM", "url_id": 111717519},
    "RIO CAL": {"idata": "17B/03BM", "url_id": 111717520},
    "SAPEIRA": {"idata": "29G/01H", "url_id": 111717574},
    "SEGURA (INAG)": {"idata": "15P/02H", "url_id": 111717523},
    #"SENHORA DA AJUDA (EDIA)": {"idata": "21N/01", "url_id": XXXXXXXXXXXXXX},
    "SOBRAL/FERREIRÓS": {"idata": "11I/08H", "url_id": 111717513},
    "SOUTO": {"idata": "06G/06", "url_id": 111717503},
    "SÃO GIÃO": {"idata": "11K/03", "url_id": 111717514},
    #"SÃO JOÃO": {"idata": "03F/04", "url_id": XXXXXXXXXXXXXX},
    "SÃO ROMÃO DO SADO": {"idata": "24G/02H", "url_id": 111717563},
    "SÃO TOMÉ": {"idata": "11E/01", "url_id": 111717511},
    "TAIPAS": {"idata": "05G/06", "url_id": 111717484},
    "VALADA": {"idata": "03D/02", "url_id": 111717476},
    "VALE DE MENDIZ": {"idata": "06L/02H", "url_id": 111717496},
    "VENDINHA": {"idata": "23K/01H", "url_id": 111717553},
    "VIDIGAL": {"idata": "30F/02H", "url_id": 111717575},
    "VIDIGAL/LIS": {"idata": "15E/09", "url_id": 111717518},
    "VILARINHO": {"idata": "03M/04H", "url_id": 111717498},
    "VIZELA SANTO ADRIÃO": {"idata": "05H/02H", "url_id": 111717487},
    "XÉVORA": {"idata": "19O/02H", "url_id": 111717550}
    #"ÁGUEDA": {"idata": "07P/03", "url_id": XXXXXXXXXXXXXX}
}

# Request delay in seconds (to avoid problems)
web_query_delay = 5

historic_dir = os.path.join(os.getcwd(), "data/data_autoExtract/historic")
last_week_dir = os.path.join(os.getcwd(), "data/data_autoExtract/last_week")

def extract_snirh_main(PARAMETER_LIST, PERIOD):
    """
    #################################################################
    Process it for mapping in App
    #################################################################
    """
    param_name_list = PARAMETER_LIST.keys()

    # Load data -> convert to dictionary
    for paramNam in param_name_list:

        # Extract data
        snirh_data = snirh_extract({paramNam: PARAMETER_LIST[paramNam]},
                                   PERIOD)

        # Move last week's data to historic
        lastweek_dataFiles = os.listdir(last_week_dir)
        for file in lastweek_dataFiles:
            if file.endswith('.npy'):
                shutil.move(os.path.join(last_week_dir, file), os.path.join(historic_dir, file))

        # Save new (last week) data
        np.save(os.path.join(last_week_dir, \
                             f'snirh_data_param{str(PARAMETER_LIST[paramNam])}_{PERIOD["tmin"].replace("/","-")}_{PERIOD["tmax"].replace("/","-")}.npy'),\
                            snirh_data)


def snirh_extract(PARAMETER, PERIOD):

    # Get geolocations
    geoStatData = pd.read_csv('data/rede_Qualidadeautomatica.csv', index_col=0, squeeze=True, encoding = "ISO-8859-1").to_dict()

    station_names = list(STATIONS.keys())
    parameter_names = list(PARAMETER.keys())
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
            param_url_id = PARAMETER[parmName]

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
                    time.sleep(web_query_delay)  # Seconds
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
                        print(f"-> (mode_1=SNIRH)(data_exists=TRUE) Data extracted for Station {idata} ({statName}) \
                        on Parameter {parmName}")
                    else:
                        print(f"-> (mode_1=SNIRH)(data_exists=FLASE) Data extracted for Station {idata} ({statName}) \
                        on Parameter {parmName}")

                except:
                    print(f"-> (mode_1=SNIRH) Problem with Station {idata} ({statName}) on Parameter {parmName}")

            snirh_data[parmName] = df_parameter

        except:
            print(f"-> (mode_1=SNIRH) Problem with Parameter {parmName} (general)")


    return snirh_data
