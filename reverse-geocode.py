import pandas as pd
import os
from helpers import log_update, tuplemaker
import reverse_geocoder as rg


def get_country_from_coordinates(latitude, longitude, row_no):
    print(row_no, end='', flush=True)
    results = rg.search((latitude, longitude))[0]
    admin_1 = results['admin1']
    admin_2 = results['admin2']
    print('\r', end='', flush=True)
    print(f" {admin_1}, {admin_2}", end='', flush=True)
    print('\r', end='', flush=True)
    return f"{admin_1}, {admin_2}"


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
        iteration_no = len(os.listdir(target_path + "/" + folder + "/"))

        for i in range(1, iteration_no):
            csv_input = target_path + "/" + folder + f"/batch_no_{i}.csv"
            df = pd.read_csv(csv_input)

            if 'admin_1' not in df.columns:
                df['input'] = df.apply(lambda row: tuplemaker(
                    row['tile_y'], row['tile_x']), axis=1)

                admin_tuples = df['input'].apply(lambda x: (x)).tolist()
                admin_raw = rg.search(admin_tuples)

                admin_1_list = [item['admin1'] for item in admin_raw]
                df['admin_1'] = admin_1_list

                admin_2_list = [item['admin2'] for item in admin_raw]
                df['admin_2'] = admin_2_list
                df.to_csv(csv_input, index=False)
                log_update(target_path, folder, csv_input)
            else:
                print(f"Skipping {csv_input}. Proceeding to next file.")


if __name__ == "__main__":
    main()
