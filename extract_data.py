#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd

df = pd.read_parquet("2023-04-01_performance_mobile_tiles.parquet")

MIN_LATITUDE_T = 4.90652903432457
MAX_LATITUDE_B = 20.906529034
MIN_LONGITUDE_L = 116.790348960255
MAX_LONGITUDE_R = 126.831282075647

EXCLUDE_MAX_LATITUDE = 7.6780609952049295
EXCLUDE_MAX_LONGITUDE = 119.61264505265808


filt_1_df = df[(df['tile_y'] >= MIN_LATITUDE_T) &
               (df['tile_y'] <= MAX_LATITUDE_B) &
               (df['tile_x'] >= MIN_LONGITUDE_L) &
               (df['tile_x'] <= MAX_LONGITUDE_R)]

# Raw PH
filt_1_df.to_csv("rawPH.csv", index=False)

# Refine


filt_2_df = filt_1_df[
    (filt_1_df['tile_y'] < EXCLUDE_MAX_LATITUDE) &
    (filt_1_df['tile_x'] < EXCLUDE_MAX_LONGITUDE)
]

filt_2_df.to_csv("rawPH_2.csv", index=False)


# In[ ]:


result = pd.merge(
    filt_1_df, filt_2_df['quadkey'], on='quadkey', how='outer', indicator=True)

ph_df = result[result['_merge'] == 'left_only'].drop(columns=['_merge'])

ph_df.to_csv("rawPH_3.csv", index=False)


# In[ ]:


ph_df.head()


# In[ ]:


latitude = 20.7895
longitude = 121.8411


api_url = f"https://nominatim.openstreetmap.org/reverse?lat={latitude}&lon={longitude}&format=jsonv2"
response = requests.get(api_url)

if response.status_code == 200:
    data = response.json()
    # Access the geocoding information from the JSON response
    print(data['address'])
else:
    print("Error occurred while fetching the data.")


# In[ ]:


df_iter = pd.read_csv("rawPH_3.csv", iterator=True, chunksize=500)
batch_no = 1

while True:
    try:
        batch_df = next(df_iter)
        batch_df.to_csv(f"For Reverse Geocoding/batch_no_{batch_no}.csv")
        batch_no += 1
    except StopIteration:
        print("gg")
        break


# In[ ]:
