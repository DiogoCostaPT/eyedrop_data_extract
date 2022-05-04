
import data_webExtract as dwe
import numpy as np
from datetime import date
import data_webExtract as WextSnirh

PARAMETER_LIST = {
    #"Condutividade (uS/cm) (uS/cm)": 100003231,
    #"Oxigénio Dissolvido - Campo (Meio) (mg/l)": 898486382,
    #"Temperatura da Água (Meio) (°C)": 898486250,
    #"Condutividade de Campo a 25ºC (Meio) (uS/cm)": 898511730,
    #"Condutividade média (6 horas) (uS/cm)": 114483358,
    #"Oxigénio Dissolvido - Campo % (Fundo) ((%))": 898511296,
    #"Oxigénio Dissolvido - Campo % (Meio) ((%))": 898511290,
    "Oxigénio Dissolvido - Campo (Fundo) (mg/l)": 898511284,
    "Oxigénio Dissolvido - Campo (Meio) (mg/l)": 898486382,
    "Oxigénio dissolvido - campo (%) ((%))": 100003496,
    "Oxigénio dissolvido - campo (mg/l O2) (mg/l)": 100003502,
    "Oxigénio dissolvido médio - campo (%) (6 horas) ((%))": 114483387,
    "Oxigénio dissolvido médio - campo (6 horas) (mg/l)": 114483369,
    "Oxigénio dissolvido mínimo diário (calculado) ((%))": 255382384,
    "Potencial REDOX (mV)": 716913332,
    "Potêncial Redox (Fundo) (mV)": 898486014,
    "Potêncial Redox (Meio) (mV)": 898462082,
    "Temperatura Amostra (°C) (°C)": 100003625,
    "Temperatura da Água (Fundo) (°C": 898486332,
    "Temperatura média da amostra (6 horas) (°C)": 114483384,
    "Turvação (Fundo) (NTU)": 898511724,
    "Turvação (Meio) (NTU)": 898511686,
    "Turvação (NTU) (NTU)": 100003630,
    "Turvação média (6 horas) (NTU)": 114483390,
    "pH - Campo (Fundo) (-)": 898486034,
    "pH - Campo (Meio) (-)": 898486022,
    "pH - campo (-)": 100003566,
    "pH médio - campo (6 horas) (-)": 114483378
}

PERIOD = {
    "tmin": "01/01/2000",
    "tmax": date.today().strftime("%d/%m/%Y")
}

# Extract Data
WextSnirh.extract_snirh_main(PARAMETER_LIST, PERIOD)

# Data process








