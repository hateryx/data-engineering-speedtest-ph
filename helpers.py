from shapely.wkt import loads
MIN_LATITUDE_T = 4.90652903432457
MAX_LATITUDE_B = 20.906529034
MIN_LONGITUDE_L = 116.790348960255
MAX_LONGITUDE_R = 126.831282075647

EXCLUDE_MAX_LATITUDE = 7.6780609952049295
EXCLUDE_MAX_LONGITUDE = 119.61264505265808


def get_coordinates(wkt_polygon):

    # Parse the WKT polygon
    polygon = loads(wkt_polygon)

    # Extract the coordinates
    coordinates = list(polygon.exterior.coords)[0]
    longitude, latitude = coordinates

    return longitude, latitude


def tuplemaker(lat, long):
    return lat, long


def log_update(target_path, folder, csv_input):
    with open(target_path + "/" + folder + "/log.txt", "a") as f:
        log = f"Done processing: {csv_input}. \n"
        f.write(log)
        print(log)
