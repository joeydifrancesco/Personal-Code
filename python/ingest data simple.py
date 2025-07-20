# -*- coding: utf-8 -*-
"""

@author: jdifr

"""

import pandas as pd
import zipfile

path = ''

'os.chdir(path)'

zip_file_path = r''
unzip_destination = path

with zipfile.Zipfile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(unzip_destination)

csv_file_path = unzip_destination + '/' + zip_ref.namelist()[0]

df = pd.read_csv(csv_file_path)
