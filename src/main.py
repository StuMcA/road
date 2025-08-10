from db.toid_storage_service import init_toid_table, save_toids_to_db
from services.os_features_client import OSFeaturesClient
from services.mapillary_client import MapillaryClient

def main():
    # Initialize DB and table
    init_toid_table()

    os_features_client = OSFeaturesClient()
    mapillary_client = MapillaryClient()

    # os_bbox = [minLat, minLon, maxLat, maxLon]
    bbox = [55.860362,-3.367996,55.995002,-2.964249] # covers Edinburgh
    # map_bbox = [minLon, minLat, maxLon, maxLat]
    map_bbox = [bbox[1],bbox[0],bbox[3],bbox[2]]
    # Fetch data (e.g. from the OS Linked Identifiers API)
    # data = os_features_client.fetch_all_toid_features(bbox, 100)
    # Insert into DB
    # save_toids_to_db(data)

    images = mapillary_client.fetch_images(bbox=map_bbox, limit=5)

    # Print metadata
    mapillary_client.print_images(images)

    # Download thumbnails
    mapillary_client.download_images(images)

if __name__ == "__main__":
    main()
