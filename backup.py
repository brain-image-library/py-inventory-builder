import uuid
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

###############################################################################################################
print(f"Processing summary metadata from brainimagelibrary.org")
url = "https://submit.brainimagelibrary.org/search/summarymetadata"

report_output_directory = '/bil/data/inventory/daily'
if not Path(report_output_directory).exists():
    Path(report_output_directory).mkdir()

now = datetime.now()
report_output_filename = report_output_directory + '/' + str(now.strftime('%Y%m%d')) + '.csv'
temp_file = Path(report_output_filename)

if temp_file.exists():
    temp_file.unlink()

response = requests.get(url)
temp_file.write_bytes(response.content)
