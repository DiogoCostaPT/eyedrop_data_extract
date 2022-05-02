
import data_webExtract as dwe
import numpy as np


"""
#################################################################
Processing data
#################################################################
"""

# Load data -> convert to dictionary
snirh_data = dwe.snirh_extract()

# Save data to npy
np.save('../web_app/assets/snirh_data.npy', snirh_data)
