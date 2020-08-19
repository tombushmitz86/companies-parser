import re
import urllib.request as request
import json
import ijson
import requests
from pandas.io.json import json_normalize
import pandas as pd
from tqdm import tqdm

COMPANIES_ENDPOINT = "https://pdl-canonical-data.s3-us-west-2.amazonaws.com/person/v10.0/company_v10.0_full.json"
COMPANY_SIZE_ENUM_ENDPOINT = "https://pdl-canonical-data.s3-us-west-2.amazonaws.com/person/v10.0/company_size.txt"


def main(args=None):
    # Populate ENUM
    response = request.urlopen(COMPANY_SIZE_ENUM_ENDPOINT)
    ranges = []
    for line in response:
        decoded_range = line.decode("utf-8")
        splitted = re.split('-|\+', decoded_range)
        # fallback for 1000+
        if splitted[1] == '':
            splitted[1] = splitted[0]
        ranges.append(tuple(map(int, splitted)))
    
    entries = []
    f = request.urlopen(COMPANIES_ENDPOINT)
    objects = ijson.items(f, '', multiple_values=True)


    limit = 10000
    iterator_count = 0
    with tqdm(total=limit) as pbar:
        for obj in objects:
            if iterator_count > limit:
                break
            pbar.set_description(f'Fetching entry {iterator_count}')
            entries.append(obj)
            iterator_count += 1
            pbar.update(1)
    
if __name__ == "__main__":
    main()