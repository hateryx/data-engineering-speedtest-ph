#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import reverse_geocoder as rg
from helpers import (EXCLUDE_MAX_LATITUDE, EXCLUDE_MAX_LONGITUDE,
                     MAX_LATITUDE_B, MAX_LONGITUDE_R, MIN_LATITUDE_T,
                     MIN_LONGITUDE_L,
                     get_coordinates_x, get_coordinates_y, tuplemaker, evaluate_dl_speed)


def main():
    input_parquet_path = "./parquet_files/"
    output_parquet_path = "./f_parquet_files/"

    dir_list = os.listdir(input_parquet_path)

    if not os.path.isdir(output_parquet_path):
        os.mkdir(output_parquet_path)

    for file in dir_list:
        print(f"Processing {input_parquet_path + file}")
        ph_df = extract_all_ph_coordinates(input_parquet_path, file)
        rg_df = reverse_geocode(ph_df)
        refined_df = label_data_rows(rg_df, file)
        refined_df.to_parquet(output_parquet_path + file)


def extract_all_ph_coordinates(path, file):

    target_path = path + file
    df = pd.read_parquet(target_path)
    print(len(df))

    if 'tile_x' not in df.columns or 'tile_y' not in df.columns:

        df['tile_x'] = df.apply(lambda row:
                                get_coordinates_x(row['tile']), axis=1, result_type='expand')

        df['tile_y'] = df.apply(lambda row:
                                get_coordinates_y(row['tile']), axis=1, result_type='expand')

    '''First filter'''
    filt_1_df = df[(df['tile_y'] >= MIN_LATITUDE_T) &
                   (df['tile_y'] <= MAX_LATITUDE_B) &
                   (df['tile_x'] >= MIN_LONGITUDE_L) &
                   (df['tile_x'] <= MAX_LONGITUDE_R)]

    '''Second filter'''
    filt_2_df = filt_1_df[
        (filt_1_df['tile_y'] < EXCLUDE_MAX_LATITUDE) &
        (filt_1_df['tile_x'] < EXCLUDE_MAX_LONGITUDE)
    ]

    result = pd.merge(
        filt_1_df, filt_2_df['quadkey'], on='quadkey', how='outer', indicator=True)

    ph_df = result[result['_merge'] == 'left_only'].drop(columns=['_merge'])

    print(f"{len(ph_df)}")

    return ph_df


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
    main_df.drop('input', axis=1, inplace=True)

    return main_df


def label_data_rows(df: pd.DataFrame, file):

    cover_date = file.split("_")[0]
    df['dl_speed_level'] = pd.Timestamp(cover_date)
    df['dl_speed_level'] = df.apply(
        lambda row: evaluate_dl_speed(row['avg_d_kbps']), axis=1)
    return df


if __name__ == "__main__":
    main()
