import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.point import Point
import os
from helpers import log_update


def get_country_from_coordinates(latitude, longitude, row_no):
    print(row_no, end='', flush=True)
    geolocator = Nominatim(user_agent="reverse_geocoder")
    geocode = RateLimiter(geolocator.reverse, min_delay_seconds=1)
    location = geocode(Point(latitude, longitude), exactly_one=True)
    print('\r', end='', flush=True)
    if location:
        address = location.raw.get('address', {})
        city = address.get('city', None)
        state = address.get('state', None)
        return f"{state},{city}"
    else:
        return None


def merge_and_clean_up(processed_files_path):
    items = os.listdir(processed_files_path)
    csv_files = [csv_file for csv_file in items if ".csv" in csv_file]

    main_df = pd.read_csv(csv_file[0])

    for csv_file in csv_files[1:]:
        append_df = pd.read_csv(csv_file)
        main_df = pd.concat([main_df, append_df], ignore_index=True)

    main_df[['state', 'city']] = main_df['city_state'].str.split(
        ',', expand=True)


def main():
    target_path = "./raw_csv_files"
    items = os.listdir(target_path)
    folders = [item for item in items if os.path.isdir(
        os.path.join(target_path, item))]

    for folder in folders:
        iteration_no = len(os.listdir(target_path + "/" + folder + "/")) + 1

        for i in range(1, iteration_no):
            csv_input = target_path + "/" + folder + f"/batch_no_{i}.csv"
            df = pd.read_csv(csv_input)

            if 'city_state' not in df.columns:
                df['city_state'] = df.apply(lambda row:
                                            get_country_from_coordinates(
                                                row['tile_y'], row['tile_x'], row.name)
                                            if row is not None else None, axis=1)
                no_location_count = len(df[df['city_state'] == 'None,None'])
                df.to_csv(csv_input, index=False)
                log_update(target_path, folder, csv_input, no_location_count)
            else:
                print(f"Skipping {csv_input}. Proceeding to next file.")


if __name__ == "__main__":
    main()
