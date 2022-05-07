
import pandas as pd

def dataIngest(DATABASE):
    """
    Extract parameter and stations data (metadata and server data)
    Preform small processing to easy access to data
    """

    # Extract station and (available) parameters metadata/info
    if (DATABASE is "infreq"):
        stationInfo = pd.read_csv('data/stationMetadataServer/stationInfo_Qualidade.csv', sep=',',
                                       encoding='ISO-8859-1')
        idServer_station = pd.read_csv('data/stationMetadataServer/idServer_station_Qualidade.txt', sep='; ')
        idServer_param = pd.read_csv('data/stationMetadataServer/idServer_param_Qualidade.txt', sep='; ')
    elif (DATABASE is "auto"):
        stationInfo = pd.read_csv('data/stationMetadataServer/stationInfo_QualidadeAUTO.csv', sep=',',
                                           encoding='ISO-8859-1')
        idServer_station = pd.read_csv('data/stationMetadataServer/idServer_station_QualidadeAUTO.txt', sep='; ')
        idServer_param = pd.read_csv('data/stationMetadataServer/idServer_param_QualidadeAUTO.txt', sep='; ')

    """
    Adding station "codigo" for lookup in station_info (lat and long)
    """
    # Station Qual (non automatic)
    if (DATABASE is "infreq"):
        statName = idServer_station["station_name"]
        stat_codigo = [statName[i][statName[i].rfind("(") + 1:statName[i].rfind(")")] for i in range(len(statName))]
        idServer_station["stat_codigo"] = stat_codigo
    elif (DATABASE is "auto"):
        # Station Qual (automatic)
        statName = idServer_station["station_name"]
        stat_codigo = [statName[i][statName[i].rfind("(") + 1:statName[i].rfind(")")] for i in range(len(statName))]
        idServer_station["stat_codigo"] = stat_codigo

    StatPram_GLOBALmeta = {
        "stationInfo": stationInfo,
        "idServer_station": idServer_station,
        "idServer_param": idServer_param
    }

    return StatPram_GLOBALmeta
