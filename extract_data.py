#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import reverse_geocoder as rg
from helpers import (EXCLUDE_MAX_LATITUDE, EXCLUDE_MAX_LONGITUDE,
                     MAX_LATITUDE_B, MAX_LONGITUDE_R, MIN_LATITUDE_T,
                     MIN_LONGITUDE_L, get_coordinates, tuplemaker)


def main():
    parquet_path = "./parquet_files/"
    csv_path = "./raw_csv_files/"

    dir_list = os.listdir(parquet_path)
    if not os.path.isdir(csv_path):
        os.mkdir(csv_path)

    for file in dir_list:
        ph_df = convert_to_csv(parquet_path, file)
        rg_df = reverse_geocode(ph_df)
        # slice_to_chunks(csv_path, file)


def convert_to_csv(path, file):
    target_path = path + file
    output_path = "./raw_csv_files/"

    df = pd.read_parquet(target_path)

    if 'tile_x' not in df.columns or 'tile_y' not in df.columns:
        df[['tile_x', 'tile_y']] = df.apply(lambda row:
                                            get_coordinates(row['tile']), axis=1)

    filt_1_df = df[(df['tile_y'] >= MIN_LATITUDE_T) &
                   (df['tile_y'] <= MAX_LATITUDE_B) &
                   (df['tile_x'] >= MIN_LONGITUDE_L) &
                   (df['tile_x'] <= MAX_LONGITUDE_R)]

    # Raw PH
    filt_2_df = filt_1_df[
        (filt_1_df['tile_y'] < EXCLUDE_MAX_LATITUDE) &
        (filt_1_df['tile_x'] < EXCLUDE_MAX_LONGITUDE)
    ]

    result = pd.merge(
        filt_1_df, filt_2_df['quadkey'], on='quadkey', how='outer', indicator=True)

    ph_df = result[result['_merge'] == 'left_only'].drop(columns=['_merge'])

    # filename = file.split(".")[0]

    # ph_df.to_csv(output_path + filename + ".csv", index=False)

    print(f"{len(ph_df)}")

    return ph_df


# def slice_to_chunks(csv_path, file):
#     output_path = csv_path + file.split(".")[0]

#     if not os.path.isdir(output_path):
#         os.mkdir(output_path)

#     file = file.split(".")[0] + ".csv"

#     df_iter = pd.read_csv(csv_path + file, iterator=True, chunksize=500)
#     batch_no = 1

#     while True:
#         try:
#             batch_df = next(df_iter)
#             batch_df.to_csv(
#                 f"{output_path}/batch_no_{batch_no}.csv", index=False)
#             batch_no += 1
#         except StopIteration:
#             print(f"Done. Processed a total of {batch_no}")
#             break

#     print(f"{file} successfully sliced to chunks...")


def reverse_geocode(df: pd.DataFrame):

    chunksize = 500
    df.loc[:, 'admin_1'] = ''
    df.loc[:, 'admin_2'] = ''
    main_df = pd.DataFrame(columns=df.columns)

    for start in range(0, len(df), chunksize):
        chunk_df = df[start: start + chunksize].copy()

        chunk_df['input'] = df.apply(lambda row: tuplemaker(
            row['tile_y'], row['tile_x']), axis=1)

        admin_tuples = chunk_df['input'].apply(lambda x: (x)).tolist()
        admin_raw = rg.search(admin_tuples)

        admin_1_list = [item['admin1'] for item in admin_raw]
        chunk_df['admin_1'] = admin_1_list

        admin_2_list = [item['admin2'] for item in admin_raw]
        chunk_df['admin_2'] = admin_2_list

        main_df = pd.concat([main_df, chunk_df], ignore_index=True)
        print(f"Added {start} to {start + chunksize}")

    print(f"{len(main_df)}")
    main_df.to_csv("test.csv")
    return main_df


if __name__ == "__main__":
    main()
