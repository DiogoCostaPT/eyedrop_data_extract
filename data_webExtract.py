
import numpy as np
import requests
import pandas as pd
import time
import shutil
import os
import ingest_MetaData

# Exemplot SNIRH
#https://snirh.apambiente.pt/snirh/_dadosbase/site/paraCSV/dados_csv.php?sites=111717548&pars=898486382&tmin=30/06/2000&tmax=18/04/2022&formato=csv


# Request delay in seconds (to avoid problems)
web_query_delay = 3

historic_dir = os.path.join(os.getcwd(), "data/data_autoExtract/historic")
last_week_dir = os.path.join(os.getcwd(), "data/data_autoExtract/last_week")


def extract_snirh_main(PERIOD, DATABASE):
    """
    #################################################################
    Process it for mapping in App
    #################################################################
    """

    # Ingest metadata and server ids
    StatPram_GLOBALmeta = ingest_MetaData.dataIngest(DATABASE)

    # Log file name
    extract_logFile = os.path.join(last_week_dir,
                                   f"Web_Extract_Log_{DATABASE}_{PERIOD['tmin'].replace('/','-')}_"
                                   f"{PERIOD['tmax'].replace('/','-')}.txt")

    # Initiate Log file and write
    logFile_obj = open(extract_logFile, 'w')
    logFile_obj.write("\n##################################################\n")
    logFile_obj.write(f"SNIRH Weekly Web Extraction\n")
    logFile_obj.write(f"-> Week period: {PERIOD['tmin']} to {PERIOD['tmax']}\n")
    logFile_obj.write("##################################################\n")
    logFile_obj.close()

    #param_name_list = PARAMETER_LIST.keys()
    param_name_list = StatPram_GLOBALmeta["idServer_param"]

    # Load data -> convert to dictionary
    for parI in range(len(param_name_list)):

        # Extract data
        snirh_data = snirh_extract(StatPram_GLOBALmeta,
                                   PERIOD,
                                   extract_logFile,
                                   param_name_list.iloc[parI]) # Provide specific parameters to retrieve

        # Move last week's data to historic
        lastweek_dataFiles = os.listdir(last_week_dir)
        for file in lastweek_dataFiles:
            if (file.endswith('.npy') or file.endswith('.log')):
                shutil.move(os.path.join(last_week_dir, file), os.path.join(historic_dir, file))

        # Save new (last week) data
        np.save(os.path.join(last_week_dir, \
                             f'snirh_data_param{param_name_list["parameter"].iloc[parI]}' \
                             f'_{PERIOD["tmin"].replace("/","-")}_{PERIOD["tmax"].replace("/","-")}.npy'),\
                            snirh_data)


def snirh_extract(PStatPram_GLOBALmeta,
                  PERIOD,
                  extract_logFile,
                  PARAMETER):

    # Initiate Log file and write
    logFile_obj = open(extract_logFile, 'a')
    logFile_obj.write(f"\n-> Parameter: {PARAMETER}\n")

    # Break down infomation in different DB
    stationInfo = PStatPram_GLOBALmeta["stationInfo"]
    idServer_station = PStatPram_GLOBALmeta["idServer_station"]
    idServer_param = PStatPram_GLOBALmeta["idServer_param"]

    station_names = list(stationInfo[stationInfo.columns[1]])
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

    try:

        parmName = PARAMETER["parameter"]
        param_url_id = PARAMETER["id_server"]

        # Creating an empty Dataframe for each parameter
        df_parameter = pd.DataFrame([])

        # Loop over stations
        for stat_i in range(len(stationInfo)):

            try:
                # Station code
                idata = stationInfo["idata"][stat_i]

                # Station geo
                station_lat = stationInfo[stationInfo.columns[3]][stat_i]
                station_lon = stationInfo[stationInfo.columns[4]][stat_i]

                station_url_id = int(idServer_station.loc[idServer_station['stat_codigo'] == idata]["id_server"][0])
                station_name = str(idServer_station.loc[idServer_station['stat_codigo'] == idata]["station_name"][0])

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
                                                DFcolumnHeaders[2]: [station_name for i in range(len(datetime))],
                                                DFcolumnHeaders[3]: [station_lat for i in range(len(datetime))],
                                                DFcolumnHeaders[4]: [station_lon for i in range(len(datetime))],
                                                DFcolumnHeaders[5]: data})

                # Append to master parameter dataframe

                if not df_Station_entry.empty:
                    df_parameter = df_parameter.append(df_Station_entry, ignore_index=True)
                    extMsg = f"-> (mode_1=SNIRH)(data_exists=TRUE) Data extracted for Station {idata} ({statName})" \
                             f"on Parameter {parmName}"
                    print(extMsg)
                    logFile_obj.write(f"{extMsg}\n")

                else:
                    extMsg = f"-> (mode_1=SNIRH)(data_exists=FLASE) Data extracted for Station {idata} ({statName}) " \
                             f"on Parameter {parmName}"
                    print(extMsg)
                    logFile_obj.write(f"{extMsg}\n")

            except:
                extMsg = f"-> (mode_1=SNIRH) Problem with Station {idata} ({station_name}) on Parameter {parmName}"
                print(extMsg)
                logFile_obj.write(f"{extMsg}\n")

        snirh_data[parmName] = df_parameter

    except:
        extMsg = f"-> (mode_1=SNIRH) Problem with Parameter {parmName} (general)\n"
        print(extMsg)
        logFile_obj.write(f"{extMsg}\n")

    logFile_obj.close()

    return snirh_data
