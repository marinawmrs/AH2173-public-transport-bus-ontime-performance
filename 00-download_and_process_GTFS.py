import pandas as pd
import subprocess
from google.transit import gtfs_realtime_pb2
from protobuf_to_dict import protobuf_to_dict
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
import requests
import time
from datetime import datetime

API_KEY = "..."
TMP_DIR = "./tmp"
STATIC_DATA_DIR = f"{TMP_DIR}/static_data"
TRIP_UPDATES_DIR = f"{TMP_DIR}/trip_updates"
UNZIPPED_STATIC_DATA_DIR = f"{STATIC_DATA_DIR}/unzipped/"
UNZIPPED_TRIP_UPDATES_DIR = f"{TRIP_UPDATES_DIR}/unzipped/"

def unzip_data(compressed_file, destination_dir):
  try:
      subprocess.run(['7z', 'x', compressed_file, f'-o{destination_dir}'], check=True)
      print(f"Successfully extracted {compressed_file} to {destination_dir}")
     
      # Move all files to the top-level directory
      for root, dirs, files in os.walk(destination_dir):
        for file in files:
          file_path = os.path.join(root, file)
          top_level_path = os.path.join(destination_dir, file)
          if file_path != top_level_path: 
            shutil.move(file_path, destination_dir)
      
      # Remove empty directories
      for root, dirs, files in os.walk(destination_dir, topdown=False):
        for dir in dirs:
          dir_path = os.path.join(root, dir)
          if not os.listdir(dir_path):
            os.rmdir(dir_path)

  except subprocess.CalledProcessError as e:
      print(f"Error during extraction: {e}")

# unzip_data('./tmp/trip-updates/sl-TripUpdates-2021-01-12-08.7z', './tmp/trip-updates/unzipped/')

def get_route_4_trips():
  """
  Gets all the trips for bus route 4 for the entire day
  Returns a dataframe with trip_id and direction_id
  """
  
  routes_df = pd.read_csv(UNZIPPED_STATIC_DATA_DIR + "routes.txt")
  route_4_id = routes_df[routes_df["route_short_name"] == "4"]["route_id"].values[0]
  
  trips_df = pd.read_csv(UNZIPPED_STATIC_DATA_DIR + "trips.txt")
  trips_df['trip_id'] = trips_df['trip_id'].astype(int)
  return trips_df[trips_df["route_id"] == route_4_id][["trip_id", "direction_id"]], route_4_id


def process_trip_updates_pb(file_path, route_4_trips_df, route_4_id):
  """
  Takes a file path to the protobuf file to process
  Returns a dataframe with trip_id, stop_id, stop_sequence, arrival_time, and delay
  """
  trip_update_feed = gtfs_realtime_pb2.FeedMessage()
  # print("Processing: ", file_path) 

  with open(file_path, "rb") as f:
      trip_update_feed.ParseFromString(f.read())

  trip_update_dict = protobuf_to_dict(trip_update_feed)
  rows = []
  missing_trip_ids = []
  missing_arrivals = []
  missing_departures = []

  for entity in trip_update_dict.get("entity", []):
    trip_update = entity.get("trip_update")
    trip = trip_update.get("trip")
    trip_id = trip.get("trip_id")

    if not trip_id:
      # print("No trip_id found in trip update for route", trip.get("route_id"), trip)
      if trip.get("route_id") == route_4_id:
        print("MATCH")
      continue
    trip_id = int(trip_id)

    if trip_id in route_4_trips_df["trip_id"].values:
      for stop_time_update in trip_update.get("stop_time_update"):
        arrival = stop_time_update.get("arrival")
        departure = stop_time_update.get("departure")
        trip_schedule_relationship = trip.get("schedule_relationship")
        stop_sequence = stop_time_update.get("stop_sequence")
        update_timestamp = trip_update.get("timestamp")

        if not arrival:
          missing_arrivals.append({
              "trip_id": trip_id,
              "stop_sequence": stop_sequence,
              "stop_id": stop_time_update.get("stop_id"),
              "stop_schedule_relationship": stop_time_update.get("schedule_relationship"),
              "update_timestamp": trip_update.get("timestamp"),
              "trip_schedule_relationship": trip_schedule_relationship,
          })
        elif not departure:
          missing_departures.append({
              "trip_id": trip_id,
              "stop_sequence": stop_sequence,
              "stop_id": stop_time_update.get("stop_id"),
              "stop_schedule_relationship": stop_time_update.get("schedule_relationship"),
              "update_timestamp": trip_update.get("timestamp"),
              "trip_schedule_relationship": trip_schedule_relationship,
          })
        else:
          rows.append({
              "trip_id": trip_id,
              "stop_sequence": stop_time_update.get("stop_sequence"),
              "stop_id": stop_time_update.get("stop_id"),
              "arrival_time": arrival.get("time"),
              "arrival_delay": arrival.get("delay"),
              "arrival_uncertainty": arrival.get("uncertainty"),
              "departure_time": departure.get("time"),
              "departure_delay": departure.get("delay"),
              "departure_uncertainty": departure.get("uncertainty"),
              "update_timestamp": trip_update.get("timestamp"),
              "trip_schedule_relationship": trip_schedule_relationship,
          })
  
  return rows, missing_arrivals, missing_departures

def process_all_trip_updates(date):
  print("Processing trip updates", date)
  route_4_trips_df, route_4_id = get_route_4_trips()
  all_rows = []
  all_missing_arrivals_rows = []
  all_missing_departures_rows = []

  for filename in os.listdir(UNZIPPED_TRIP_UPDATES_DIR):
    file_path = os.path.join(UNZIPPED_TRIP_UPDATES_DIR, filename)
    processed_rows, missing_arrivals, missing_departures = process_trip_updates_pb(file_path, route_4_trips_df, route_4_id)
    all_rows = all_rows + processed_rows
    all_missing_arrivals_rows = all_missing_arrivals_rows + missing_arrivals
    all_missing_departures_rows = all_missing_departures_rows + missing_departures

  all_route_4_arrivals = pd.DataFrame(all_rows)
  all_route_4_arrivals.merge(
      route_4_trips_df[['trip_id', 'direction_id']],  
      on='trip_id', 
      how='left'
  )
  all_route_4_arrivals.to_csv(f"./raw_data/additional_control/{date}-arrivals-and-departures.csv", index=False)

  if len(all_missing_arrivals_rows) > 0:
    all_route_4_missing_arrivals = pd.DataFrame(all_missing_arrivals_rows)
    all_route_4_missing_arrivals = all_route_4_missing_arrivals.merge(
        route_4_trips_df[['trip_id', 'direction_id']],  
        on='trip_id', 
        how='left'
    )
    all_route_4_missing_arrivals.to_csv(f"./raw_data/additional_control/{date}-missing-arrivals.csv", index=False)

  if len(all_missing_departures_rows) > 0:
    all_route_4_missing_departures = pd.DataFrame(all_missing_departures_rows)
    all_route_4_missing_departures = all_route_4_missing_departures.merge(
        route_4_trips_df[['trip_id', 'direction_id']],  
        on='trip_id', 
        how='left'
    )
    all_route_4_missing_departures.to_csv(f"./raw_data/additional_control/{date}-missing-departures.csv", index=False)

  print("Processed trip updates")

def download_file(url, output_file, retries=0):
  """
  Downloads a single file with retry logic.
  """
  for attempt in range(retries + 1):
    try:
      print(f"Downloading: {url} (Attempt {attempt + 1})")
      response = requests.get(url, stream=True)
      response.raise_for_status()  # Raise an error for bad HTTP responses
      
      # Save the file
      with open(output_file, "wb") as f:
          for chunk in response.iter_content(chunk_size=8192):
              f.write(chunk)
      
      print(f"Saved: {output_file}")
      return  # Exit the function if successful
    except requests.exceptions.RequestException as e:
      print(f"Failed to download {url}: {e}")
      if attempt < retries:
          print("Retrying in 10 seconds...")
          time.sleep(10)  # Wait 30 seconds before retrying
      else:
          print("Max retries reached. Skipping this file.")
          raise e

def download_trip_updates(date):
  """
  Downloads trip updates for the specified date and hours (6, 7, 8) in parallel.
  Saves the .7z files in the ./trip-updates folder.
  """
  print("Beginning download for ", date)
  os.makedirs(TRIP_UPDATES_DIR, exist_ok=True)
  os.makedirs(STATIC_DATA_DIR, exist_ok=True) 

  parallel_tasks = []

  # Add the real-time data download tasks
  hours = ["06", "07", "08"]
  for hour in hours:
    url = f"https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt/sl/TripUpdates?date={date}&hour={hour}&key={API_KEY}"
    output_file = f"{TRIP_UPDATES_DIR}/sl-TripUpdates-{date}-{hour}.7z"
    parallel_tasks.append((url, output_file))
  
  # Prepare the static data download tasks
  parallel_tasks.append((
    f"https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-static/sl?date={date}&key={API_KEY}",
    f"{STATIC_DATA_DIR}/GTFS-SL-{date}.7z"
  ))

  try:
    # download files in parallel
    with ThreadPoolExecutor() as executor:
      futures = [executor.submit(download_file, url, output_file) for url, output_file in parallel_tasks]

      for future in futures:
        future.result() 

    print("Download complete, unzipping")
    os.makedirs(UNZIPPED_TRIP_UPDATES_DIR, exist_ok=True)
    os.makedirs(UNZIPPED_STATIC_DATA_DIR, exist_ok=True) 

    for url, output_file in parallel_tasks:
      if os.path.exists(output_file):
        unzip_data(output_file, UNZIPPED_TRIP_UPDATES_DIR if "TripUpdates" in output_file else UNZIPPED_STATIC_DATA_DIR)

    print("Unzip complete")
    return True
  
  except Exception as e:
    print(f"Error during download or unzip: {e}")
    return False

dates_to_process = ["2021-01-04","2021-01-08","2021-01-22","2021-02-04","2021-02-05","2021-02-10","2021-03-05","2021-03-17","2021-11-09","2021-12-07","2021-12-17","2022-01-27","2022-02-04","2022-02-10","2022-11-08","2022-12-06","2022-12-19","2023-01-17","2023-02-09","2023-03-02","2023-03-13","2023-03-16","2023-03-20","2023-11-07","2024-01-11","2024-01-24","2024-01-25","2024-02-05"]

failed_dates = []

for date in dates_to_process:
  processed_file_exists = [f for f in os.listdir("./raw_data/additional_control") if f.startswith(date)]
  if processed_file_exists:
      print(f"Data for date {date} already processed. Skipping.")
      continue
  
  date_obj = datetime.strptime(date, '%Y-%m-%d')
  # Check if the date is a weekday (Monday=0, Sunday=6)
  if date_obj.weekday() < 5:  # 0-4 are weekdays
      print(f"{date} is a weekday.")
  else:
      print(f"Skipping: {date} is a weekend.")
      continue
  
  shutil.rmtree(TMP_DIR, ignore_errors=True)
  if download_trip_updates(date) == False:
    print("Failed to download trip updates for date:", date)
    failed_dates.append(date)
    continue
  process_all_trip_updates(date)
  print()

shutil.rmtree(TMP_DIR, ignore_errors=True)
print("Complete!")

if len(failed_dates) > 0:
  print("Failed to process the following dates:", failed_dates)
else:
  print("All dates processed successfully.")
