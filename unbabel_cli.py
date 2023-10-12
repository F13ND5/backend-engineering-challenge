# Import necessary modules
from argparse import ArgumentParser, ArgumentTypeError
import math
from pathlib import Path
from datetime import datetime, timedelta
import json
from statistics import mean 

# Define a function to parse command-line arguments
def parse_args():
    # Create an ArgumentParser object
    parser = ArgumentParser(
        prog="moving average of the translation delivery time", # name of program
        description="Takes a json file and a window time, loads the json, calculate for every minute, a moving average of the translation delivery time for the last X minutes and then saves the json to specified file.", # description
        epilog="By Bruno Carvalho" # bottom text
    )
    
    # add arguments
    parser.add_argument("-in", "--input_file", dest="in_file", help="The input file to change the separator in") # The input file argument
    parser.add_argument("-out", "--out_file", dest="out_file", help="The output file(s) to write to") # output file
    parser.add_argument("-w", "--window_size", dest="window_size", type=int, default=10, help="Window time") # window time
    
    return parser.parse_args()


# Function to calculate the average value
def average(lst) -> float:
	return round(mean(lst), 1) if lst else 0


# Function to check the window size value
def check_window_size(window_size):
    if window_size < 0 or not isinstance(window_size, int):
        raise ArgumentTypeError("'window_size' must be a positive integer.")


# Function to load the input data from a file
def load_data_from_file(file_path):
    with open(file_path) as f:
        data = json.load(f)
    return data


# Function to verify and save the output to a file
def save_payload_to_file(out_file, overwrite, payload):

    if not out_file:
        out_file = "output.json"
 
    if not overwrite and Path(out_file).exists():
        raise ValueError("overwrite was not specified and out-file already exists.")
 
    with open(out_file, "w") as output_file:
        json.dump(payload, output_file, indent=1)


# Function to calculate the moving average delivery time
def calculate_moving_average(data, window_size):
    
    start_time = datetime.strptime(data[0]['timestamp'],'%Y-%m-%d %H:%M:%S.%f') # Timestamp first translation
    end_time = datetime.strptime(data[-1]['timestamp'],'%Y-%m-%d %H:%M:%S.%f') # Timestamp last translation
    delta = (end_time - start_time).total_seconds() /60 # Number of minutes between first and last translation
    time_window = timedelta(minutes = window_size)

    lst_durations = [] # Stores relevant translation durations 
    final_payload = [] # Stores final data to dump in json file

    index_append  = 0 # Index in list pointing at the next translation to be included in the average
    index_pop = 0 # Index in list  pointing at the at next translation to be removed from the average

    WINDOW_INCREMENT = 1
    MINUTE_INCREMENT = 1

    #delta = delta + 1 if delta < (end_time.minute - start_time.minute) else delta
		
	# For each minute, check if translation event should be either included or excluded
    for minute in range(math.ceil(delta) + WINDOW_INCREMENT):
        
        curr_time = start_time.replace(second=0, microsecond=0) + timedelta(minutes=minute)

		# Selecting the next translation to be included in the average
        for d in data[index_append:]:

            time = datetime.strptime(d['timestamp'],'%Y-%m-%d %H:%M:%S.%f')

            if curr_time >= time:
                lst_durations.append(d['duration'])
                index_append += 1
            else:
                break

		# Selecting the next translation to be removed from the average
        for d in data[index_pop:]:

            time = datetime.strptime(d['timestamp'],'%Y-%m-%d %H:%M:%S.%f')

            if (curr_time - time_window) >= time:
                lst_durations.pop(0)
                index_pop += 1
            else:
                break
		
        # Add and structure the data for output
        event = {"date" : str(curr_time), "average_delivery_time" : average(lst_durations)}
        final_payload.append(event)

        curr_time += timedelta(minutes = MINUTE_INCREMENT)
        
    return final_payload
    

def main(in_file, out_file, window_size, overwrite=False):
    
    data = load_data_from_file(in_file)
    check_window_size(window_size)
    payload = calculate_moving_average(data, window_size)
    save_payload_to_file(out_file, overwrite, payload)


if __name__ == "__main__":
   
    args = parse_args()
    main(**vars(args))