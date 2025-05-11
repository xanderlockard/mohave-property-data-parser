import argparse
import random
import re
import time
import requests
import pandas

ORIGINAL_COLUMNS = [
    'Parcel Number',
    'Account Number',
    'Owner',
    'Amount',
    'GIS Map Hyperlink'
]

GEOCORTEX_KEYS = [
    "PARCEL",
    "PARCEL_SIZE",
    "UNIT_TYPE",
    "SITE_ADDRESS",
    "LANDVALUE",
    "OWNER",
    "MAILING_ADDRESS",
    "CITY",
    "STATE",
    "ZIP",
    "ASSESSED_FULL_CASH_VALUE",
    "TOTAL_FCV_VALUE",
    "ASSESSED_LIMITED",
    "TOTAL_LPV_NAV",
    "EXEMPTION_TYPE",
    "EXEMPTION_AMOUNT",
    "USE_CODE",
    "LEGAL_DESCRIPTION",
    "SALEP",
    "SALEDT",
    "REC_BOOK",
    "REC_PAGE",
    "DEEDTYPE",
    "LAND_LEGAL_CLASS",
    "PROTOTYPE",
    "IMPR_LEGAL_CLASS",
    "ASSESSMENT_RATIO",
    "PARCEL_KEY",
    "CLASS_CODE",
    "RESPONSIBLE_FOR_VALUATION",
    "LIMITED_VALUE",
    "VALUE_METHOD",
    "TAX_AREA_CODE_FMT",
    "PROPCODE",
    "PROPUSE",
    "ABSTDESCR",
    "IMPVALUE",
    "TAX_YEAR",
    "TWN_RNG_SEC",
    "BOS_DISTRICT"
]

BASE_PARAMS = {
    "f": "json",
    "where": "",
    "outFields": "*",
    "returnGeometry": "false",
}

BASE_URL = "https://mcgis.mohave.gov/ArcGIS/rest/services/PARCELS/MapServer/14/query"

def get_geocortex_data(url):
    m = re.search(r"ParcelId=([^&]+)", url)
    if not m:
        raise ValueError(f"Couldn't extract ParcelId from URL: {url}")
    parcel_id = m.group(1)

    params = BASE_PARAMS.copy()
    params["where"] = f"TAXPIN = '{parcel_id}'"

    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    attrs = data['features'][0]['attributes'] if data.get('features') else {}
    values = { key: attrs.get(key) for key in GEOCORTEX_KEYS}
    if not all(value is None for value in values.values()):
        return values
    return False

def write_csv_row(original_data, geocortex_data, f):
    for col in ORIGINAL_COLUMNS:
        val = original_data.get(col, '')
        if col == 'Parcel Number':
            val = str(val).strip()
        elif col == 'Owner':
            val = str(val).rstrip()
            
        f.write(f"{val},")

    for i, key in enumerate(GEOCORTEX_KEYS):
        val = geocortex_data.get(key, '')
        text = str(val).replace(',', '')  
        end = '\n' if i == len(GEOCORTEX_KEYS) - 1 else ','
        f.write(f"{text}{end}")


def prepare_output_csv(f):
    for name in ORIGINAL_COLUMNS:
        f.write(f'{name},')
    for index,name in enumerate(GEOCORTEX_KEYS):
        if index == len(GEOCORTEX_KEYS) - 1:
            f.write(f'{name}')
        else:
            f.write(f'{name},')
    f.write('\n')

def get_output_csv(output_csv_name):
    return open(output_csv_name,'w')

def get_input_csv(input_csv_name):
    return pandas.read_csv(input_csv_name)

def parse_input_csv(input_csv: pandas.DataFrame, output_file):
    for index, row in input_csv.iterrows():
        if (index % 10 == 0 and index != 0):
            print(f"Processed {index} rows")

        geo = get_geocortex_data(row['GIS Map Hyperlink'])
        if not geo:
            print(f"Failed to fetch data for row: {index}")
            continue
        write_csv_row(row, geo, output_file)
        time.sleep(random.uniform(0,3))

def main():
    parser = argparse.ArgumentParser(description="Parse property CSV data.")
    parser.add_argument('-i', '--input_csv', type=str, required=True, help='Path to the input CSV file')
    parser.add_argument('-o','--output_csv',type=str,required=True,help='Path to output CSV file')
    args = parser.parse_args()
    
    o = get_output_csv(args.output_csv)
    prepare_output_csv(o)
    df = get_input_csv(args.input_csv)
    parse_input_csv(df, o)
    

if __name__ == "__main__":
    main()
    
