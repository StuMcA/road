import math

def bbox_from_point(lat, lon, half_side_m):
    """
    Calculate a bounding box centred at (lat, lon) with half-side in metres.
    
    Parameters:
        lat (float): Latitude in degrees
        lon (float): Longitude in degrees
        half_side_m (float): Half the width/height of the bounding box in metres
        
    Returns:
        (min_lat, min_lon, max_lat, max_lon) in degrees
    """
    # Earth radius in metres
    R = 6378137.0  
    
    # Convert to radians
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)

    # Offset in radians for given distance north/south
    dlat = half_side_m / R
    # Offset in radians for given distance east/west (adjusted by latitude)
    dlon = half_side_m / (R * math.cos(lat_rad))

    # Convert offsets back to degrees
    min_lat = math.degrees(lat_rad - dlat)
    max_lat = math.degrees(lat_rad + dlat)
    min_lon = math.degrees(lon_rad - dlon)
    max_lon = math.degrees(lon_rad + dlon)

    return min_lat, min_lon, max_lat, max_lon

