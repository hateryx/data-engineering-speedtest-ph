#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
from helpers import (EXCLUDE_MAX_LATITUDE, EXCLUDE_MAX_LONGITUDE,
                     MAX_LATITUDE_B, MAX_LONGITUDE_R, MIN_LATITUDE_T,
                     MIN_LONGITUDE_L, get_coordinates)


def main():
    path = "./parquet_files/"
    dir_list = os.listdir(path)

    for file in dir_list:
        convert_to_csv(path, file)

    csv_path = "./raw_csv_files/"

    dir_list = os.listdir(csv_path)
    for file in dir_list:
        slice_to_chunks(csv_path, file)


def convert_to_csv(path, file):
    target_path = path + file
    output_path = "./raw_csv_files/"

    df = pd.read_parquet(target_path)

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

    filename = file.split(".")[0]

    ph_df.to_csv(output_path + filename + ".csv", index=False)


def slice_to_chunks(csv_path, file):
    output_path = csv_path + file.split(".")[0]
    os.mkdir(output_path)

    df_iter = pd.read_csv(csv_path + file, iterator=True, chunksize=500)
    batch_no = 1

    while True:
        try:
            batch_df = next(df_iter)
            batch_df.to_csv(
                f"{output_path}/batch_no_{batch_no}.csv", index=False)
            batch_no += 1
        except StopIteration:
            print(f"Done. Processed a total of {batch_no}")
            break


if __name__ == "__main__":
    main()
