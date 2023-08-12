from shapely.wkt import loads
import pandas as pd

MIN_LATITUDE_T = 4.90652903432457
MAX_LATITUDE_B = 20.906529034
MIN_LONGITUDE_L = 116.790348960255
MAX_LONGITUDE_R = 126.831282075647

EXCLUDE_MAX_LATITUDE = 7.6780609952049295
EXCLUDE_MAX_LONGITUDE = 119.61264505265808


def get_coordinates(wkt_polygon):
    '''Parse the WKT polygon'''
    polygon = loads(wkt_polygon)

    # Extract the coordinates
    coordinates = list(polygon.exterior.coords)[0]
    longitude, latitude = coordinates

    return pd.Series({'tile_x': longitude, 'tile_y': latitude})


def get_coordinates_x(wkt_polygon):
    '''Parse the WKT polygon'''
    polygon = loads(wkt_polygon)
    coordinates = list(polygon.exterior.coords)[0]
    longitude = coordinates[0]

    return longitude


def get_coordinates_y(wkt_polygon):
    '''Parse the WKT polygon'''
    polygon = loads(wkt_polygon)
    coordinates = list(polygon.exterior.coords)[0]
    latitude = coordinates[1]

    return latitude


def tuplemaker(lat, long):
    return lat, long


def evaluate_dl_speed(dl):
    if dl > 45000.00:
        return "45MB/s +"
    elif dl > 35000.00:
        return "35MB/s to 45MB/s"
    elif dl > 25000.00:
        return "25MB/s to 35MB/s"
    elif dl > 10000.00:
        return "10MB/s to 25MB/s"
    else:
        return "< 10MB/s"


def evaluate_latency(ms):
    '''reference: https://www.pingplotter.com/wisdom/article/is-my-connection-good/'''
    if ms > 200:
        return "200ms +"
    elif ms > 100:
        return "100ms to 200ms"
    elif ms > 50:
        return "50ms to 100/s"
    else:
        return "0 to 50ms"


def log_update(target_path, folder, csv_input):
    with open(target_path + "/" + folder + "/log.txt", "a") as f:
        log = f"Done processing: {csv_input}. \n"
        f.write(log)
        print(log)
