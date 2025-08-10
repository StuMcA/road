import requests
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

NAMESPACES = {
    "osfeatures": "http://www.ordnancesurvey.co.uk/osfeatures/1.0",
    "gml": "http://www.opengis.net/gml",
    "wfs": "http://www.opengis.net/wfs"
}

class OSFeaturesClient:
    BASE_URL = "https://api.os.uk/features/v1/wfs"

    def __init__(self):
        self.api_key = os.getenv("OS_API_KEY")
        if not self.api_key:
            raise ValueError("OS_API_KEY not set in environment")
        self.session = requests.Session()

    def fetch_all_toid_features(self, bbox: list, count: int = 100):
        all_features = []
        offset = 0

        while True:
            print(f"Fetching page with offset {offset}...")

            response = self.fetch_toid_features(
                bbox,
                count,
                offset
            )

            all_features.extend(response)

            print('Received ', len(response), ' features')

            if len(response) < count:
                print(len(all_features), ' features found')
                break  # Last page

            offset += count
            print(len(all_features), ' found so far')

        return all_features


    def fetch_toid_features(self, bbox: list, count: int = 100, start: int = 0):
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": "OpenTOID_HighwaysNetwork",
            "srsName": "EPSG:4326",
            "outputFormat": "GEOJSON",
            "bbox": ",".join([str(i) for i in bbox]),
            "key": self.api_key,
            "count": count,
            "startIndex": start
        }
        print('Fetching ', count, ' features from index ', start)
        response = self.session.get(self.BASE_URL, params=params)

        print("Status:", response.status_code)

        if not response.ok:
            print("Error content:\n", response.text[:1000])
            response.raise_for_status()

        if "application/json" in response.headers.get("Content-Type", ""):
            return self.parse_toid_features(response.json()['features'])
        else:
            print("Unexpected response:\n", response.text[:1000])
            raise ValueError("Expected JSON but got something else.")
        
    def parse_toid_features(self, features):
        data = []
        for feature in features:
            props = feature['properties']
            geometry = feature['geometry']

            easting = props.get('Easting')
            northing = props.get('Northing')
            toid = props.get('TOID')
            version_date = props.get('VersionDate')
            source_product = props.get('SourceProduct')
            longtitude = geometry.get('coordinates')[0]
            latitude = geometry.get('coordinates')[1]

            if not (easting and northing and toid):
                continue

            try:
                parsed_date = datetime.strptime(version_date, "%m/%d/%Y").date()
            except:
                parsed_date = None

            geom_wkt = f"SRID=27700;POINT({easting} {northing})"
            data.append((toid, parsed_date, source_product, geom_wkt, longtitude, latitude))
        print('Found TOID from; ', data[0])
        return data


