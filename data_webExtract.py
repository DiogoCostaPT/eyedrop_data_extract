
import numpy as np
import requests
import pandas as pd
import time
import os
import sys
import ingest_MetaData

# Exemplot SNIRH
#https://snirh.apambiente.pt/snirh/_dadosbase/site/paraCSV/dados_csv.php?sites=111717548&pars=898486382&tmin=30/06/2000&tmax=18/04/2022&formato=csv


# Request delay in seconds (to avoid problems)
web_query_delay = 3


def extract_snirh_main(PERIOD, DATABASE, last_week_dir):
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
                                   f"{PERIOD['tmax'].replace('/','-')}.log")

    reRun_flag = False

    if not os.path.exists(extract_logFile):

        # Initiate Log file and write
        logFile_obj = open(extract_logFile, 'w')
        logFile_obj.write("\n##################################################\n")
        logFile_obj.write(f"SNIRH Weekly Web Extraction\n")
        logFile_obj.write(f"-> Week period: {PERIOD['tmin']} to {PERIOD['tmax']}\n")
        logFile_obj.write("##################################################\n")
        logFile_obj.close()

    else:
        logFile_obj = open(extract_logFile, 'a')
        logFile_obj.write("\n##################################################\n")
        logFile_obj.write("*** Restarted after process was interrupted ***\n")
        logFile_obj.write("##################################################\n")
        logFile_obj.close()
        reRun_flag = True

    param_name_list = StatPram_GLOBALmeta["idServer_param"]

    # Load data -> convert to dictionary
    for parI in range(len(param_name_list)):

        parExam = param_name_list.iloc[parI]
        parExam_idServer = parExam["id_server"]

        # Check if parameter has been examined
        with open(extract_logFile) as f:
            match_parInt = f"ParameterExtractComplete_{parExam_idServer}" in f.read().splitlines()

        # If parameter is in log and extraction process has been completed, then if yes we
        # can skip it
        if match_parInt:
            continue

        SaveResPath = os.path.join(last_week_dir, \
                f'snirh_data_{DATABASE}_param{parExam_idServer}' \
                f'_{PERIOD["tmin"].replace("/","-")}_{PERIOD["tmax"].replace("/","-")}.npy')

        # Extract data
        df_parameter = snirh_extract(StatPram_GLOBALmeta,
                            DATABASE,
                            PERIOD,
                            extract_logFile,
                            parExam,
                            parI,
                            len(param_name_list))

        # Save new (last week) data
        if not df_parameter.empty:
            np.save(SaveResPath, df_parameter)

        logFile_obj = open(extract_logFile, 'a')
        logFile_obj.write(f"ParameterExtractComplete_{parExam_idServer}")
        logFile_obj.close()


def snirh_extract(PStatPram_GLOBALmeta,
                    DATABASE,
                    PERIOD,
                    extract_logFile,
                    PARAMETER,
                    parI,
                    paramNum):



    # Initiate Log file and write
    logFile_obj = open(extract_logFile, 'a')
    logFile_obj.write(f"\n-> Parameter: {int(PARAMETER['id_server'])}\n")

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

    # Creating an empty Dataframe for each parameter
    df_parameter = pd.DataFrame([])

    try:

        parmName = PARAMETER["parameter"]
        param_url_id = PARAMETER["id_server"]

        # Loop over stations
        for stat_i in range(len(stationInfo)):

            try:
                # Station code
                idata = stationInfo["idata"][stat_i]

                # Station geo
                station_lat = stationInfo[stationInfo.columns[3]][stat_i]
                station_lon = stationInfo[stationInfo.columns[4]][stat_i]

                station_url_id = int(idServer_station.loc[idServer_station['stat_codigo'] == idata]["id_server"])
                station_name = idServer_station.loc[idServer_station['stat_codigo'] == idata]["station_name"].values[0]

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


                if not df_Station_entry.empty:
                    df_parameter = df_parameter.append(df_Station_entry, ignore_index=True)
                    extMsg = f"-> (mode_1=SNIRH)(db={DATABASE})(param=[{parI}/{paramNum}],stat=[{stat_i}/{len(stationInfo)}])[data_exists=TRUE) Data extract for Station {idata} ({station_name})" \
                             f"on Parameter {parmName}"
                    print(extMsg)
                    logFile_obj.write(f"{extMsg}\n")

                else:
                    extMsg = f"-> (mode_1=SNIRH)(db={DATABASE})(param=[{parI}/{paramNum}],stat=[{stat_i}/{len(stationInfo)}])[data_exists=FALSE] Data extract for Station {idata} ({station_name}) " \
                             f"on Parameter {parmName}"
                    print(extMsg)
                    logFile_obj.write(f"{extMsg}\n")

            except:
                extMsg = f"-> (mode_1=SNIRH)(db={DATABASE})(param=[{parI}/{paramNum}],stat=[{stat_i}/{len(stationInfo)}]) Problem with Station {idata} ({station_name}) on Parameter {parmName}"
                print(extMsg)
                logFile_obj.write(f"{extMsg}\n")

    except:
        extMsg = f"-> (mode_1=SNIRH)(db={DATABASE}) Problem with Parameter {parmName} (general)\n"
        print(extMsg)
        logFile_obj.write(f"{extMsg}\n")

    logFile_obj.close()

    return df_parameter
