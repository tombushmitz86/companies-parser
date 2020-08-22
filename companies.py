import logging
import re
import urllib.request as request
import json
import ijson
from typing import Tuple, List
import requests
from pandas.io.json import json_normalize
import pandas as pd
from tqdm import tqdm
from countrygroups import EUROPEAN_UNION
from collections import Counter
import matplotlib.pyplot as plt


COMPANIES_ENDPOINT = "https://pdl-canonical-data.s3-us-west-2.amazonaws.com/person/v10.0/company_v10.0_full.json"
COMPANY_SIZE_ENUM_ENDPOINT = "https://pdl-canonical-data.s3-us-west-2.amazonaws.com/person/v10.0/company_size.txt"

logger = None

def _get_company_size_range(company_size: int, company_size_ranges: List[Tuple[int, int]]) -> str:
    for curr_range in company_size_ranges:
        top_range = curr_range[1]
        if top_range > company_size:
            return f'{curr_range[0]}-{curr_range[1]}'
    return f'{company_size_ranges[-1][0]}+'


def main(args=None):
    # Populate ENUM
    EUROPEAN_UNION_COUNTRIES = [x.lower() for x in EUROPEAN_UNION.names]
    logger.info('fetching company size enum')
    response = request.urlopen(COMPANY_SIZE_ENUM_ENDPOINT)
    ranges = []
    for line in response:
        decoded_range = line.decode("utf-8")
        splitted = re.split('-|\+', decoded_range)
        # fallback for 1000+
        if splitted[1] == '':
            splitted[1] = splitted[0]
        ranges.append(tuple(map(int, splitted)))
    logger.info('company size enum - Populated')
    
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
            company_size = _get_company_size_range(int(obj['employee_counts']['v9_total']), ranges)  
            obj['company_size_range'] = company_size
            obj['is_europian'] =  obj['country'] in EUROPEAN_UNION_COUNTRIES
            entries.append(obj)
            iterator_count += 1
            
            pbar.update(1)
    dataframe = pd.read_json(json.dumps(entries))
    only_europian = dataframe[dataframe['is_europian'] == True]
    
    only_europian.groupby('country')['country'].count().plot(title='companies per country', kind='bar')
    plt.savefig('companies_per_country_histo.png')
    only_europian.groupby('country').count().to_csv('companies_per_country.csv', columns=['pdl_id'])


    
if __name__ == "__main__":
    logging.basicConfig()
    logging.root.setLevel(logging.NOTSET)
    logger = logging.getLogger()
    main()