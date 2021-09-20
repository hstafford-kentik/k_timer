#! /usr/bin/python3

import requests
import json
import argparse
from argparse import RawTextHelpFormatter
import sys
import datetime
import re
import time
from operator import itemgetter

parser = argparse.ArgumentParser(
		description='''k_timer.py: a cli utility to retrieve a top talkers (up to 350) and export start and end times of transactions
- Output filename required at command line
- Input file should be a copy of JSON Query as found in Kentik Data Explorer.
- Kentik email address & API token required at command line.
- If start and end times are not included in the command line, (-st and -et) then the start/end from json input query are used.
    - Dates MUST be in the format 'YYYY:MM:DD:HH:mm' zero-padded for single digit entries
- Max Idle Time is the maximum amount of time (in seconds)that a flow may be dormant before consided to end.  (for "bouncy" flows)
- Email and API info can be found in your Kentik profile (https://portal.kentik.com/profile4/info)''', formatter_class=RawTextHelpFormatter)

# Positional Arguments
parser.add_argument('output_file', help='Name of output file')

# Optional Arguments
parser.add_argument('-e','--email', help='Kentik User Email', metavar='')
parser.add_argument('-a','--api', help='API key for User', metavar='')
parser.add_argument('-if','--input_file', help='Filename [input.json]', metavar='')
parser.add_argument('-st','--start_time', help='Start time for query [json query file]', metavar='')
parser.add_argument('-et','--end_time', help='End time for query [json query file]', metavar='')
parser.add_argument('-idle','--max_idle_time', help='Maximum idle time per transfer, in seconds [60]', metavar='')
parser.add_argument('-sort','--sort_field', help='What to sort output by.  May be "key" or "start" [start]', metavar='')

# Create a variable that holds all the arguments
args = parser.parse_args()

# Assign email and api token supplied to variables to be used with API call
# If email and api not supplied check hardcoded variables on line 32 and 33
if args.email and args.api:
	email = args.email
	api = args.api
else:
	print ('You must supply your email and api key.  Use k_timer.py -h for help.') 

if args.max_idle_time:
	maxIdleTime = int(args.max_idle_time)
else:
	maxIdleTime = 60
	
if args.sort_field:
	sortBy = args.sort_field
else:
	sortBy = 'start'

# Conditional: If --input_file or -if and filename specified use file contents, else look for hardcoded Kentik query
if args.input_file:
	input_file = open(args.input_file, "r")
	json_chart_data = input_file.read()
	input_file.close()
else:
	input_file = open('input.json', "r")
	json_chart_data = input_file.read()
	input_file.close()

json_chart_data = re.sub(r'"depth": [0-9]+"','"depth": 350', json_chart_data)
json_chart_data = re.sub(r'"topx": [0-9]+"','"topx": 350', json_chart_data)

if args.end_time and args.start_time:
	startTime = args.start_time.split(':')
	startTimeEpoch = datetime.datetime(int(startTime[0]),int(startTime[1]),int(startTime[2]),int(startTime[3]),int(startTime[4])).timestamp()
	#formattedTime = startTime[0]+'-'+startTime[1]+'-'+startTime[2]+' '+startTime[3]+':'+startTime[4]
	#json_chart_data = re.sub(r'"starting_time": ".{16}"','"starting_time": "'+str(formattedTime)+'"', json_chart_data)

	endTime = args.end_time.split(':')
	endTimeEpoch = datetime.datetime(int(endTime[0]),int(endTime[1]),int(endTime[2]),int(endTime[3]),int(endTime[4])).timestamp()
	#formattedTime = endTime[0]+'-'+endTime[1]+'-'+endTime[2]+' '+endTime[3]+':'+endTime[4]
	#json_chart_data = re.sub(r'"ending_time": ".{16}"','"ending_time": "'+str(formattedTime)+'"', json_chart_data)	

	#lookback = int(endTimeEpoch-startTimeEpoch)
	#json_chart_data = re.sub(r'"from_to_lookback": .+,"','"from_to_lookback": '+str(lookback)+',', json_chart_data)	
else:
	inputData = json.loads(json_chart_data)
	startTime = inputData['queries'][0]['query']['starting_time']
	startTimeEpoch = datetime.datetime(int(startTime[0:4]),int(startTime[5:7]),int(startTime[8:10]),int(startTime[11:13]),int(startTime[-2])).timestamp()
	endTime = inputData['queries'][0]['query']['ending_time']
	endTimeEpoch = datetime.datetime(int(endTime[0:4]),int(endTime[5:7]),int(endTime[8:10]),int(endTime[11:13]),int(endTime[-2])).timestamp()

if endTimeEpoch - startTimeEpoch > 3600:
	startTimeChunk = []
	endTimeChunk = []
	for n in range(int(startTimeEpoch),int(endTimeEpoch),3600):
		startTimeChunk.append(n)
	for n in range(0,len(startTimeChunk)):
		if n == len(startTimeChunk)-1:
			endTimeChunk.append(int(endTimeEpoch))
		else:
			endTimeChunk.append(int(startTimeChunk[n])+3600)
else:
	startTimeChunk = [int(startTimeEpoch)]
	endTimeChunk = [int(endTimeEpoch)]

def prep_json_data(thisStartTimeEpoch,thisEndTimeEpoch,raw_chart_data):
	formattedTime = str(datetime.datetime.fromtimestamp(int(thisStartTimeEpoch)).strftime('%Y-%m-%d %H:%M'))
	raw_chart_data = re.sub(r'"starting_time": ".{16}"','"starting_time": "'+str(formattedTime)+'"', str(raw_chart_data))
	formattedTime = str(datetime.datetime.fromtimestamp(int(thisEndTimeEpoch)).strftime('%Y-%m-%d %H:%M'))
	raw_chart_data = re.sub(r'"ending_time": ".{16}"','"ending_time": "'+str(formattedTime)+'"', raw_chart_data)
	lookback = int(thisEndTimeEpoch-thisStartTimeEpoch)
	raw_chart_data = re.sub(r'"from_to_lookback": .+,"','"from_to_lookback": '+str(lookback)+',', raw_chart_data)	
	return raw_chart_data

# Create a function to make the API call and create the image
def data_api_call(k_email, k_api, viz_data):

	# Set appropriate headers and API end-point for JSON call
	headers = {'Content-Type': 'application/json', 'X-CH-Auth-API-Token': k_api, 'X-CH-Auth-Email': k_email}
	url = 'https://api.kentik.com/api/v5/query/topxdata'

	try:
		# Use JSON requests to pull chart and assign to a variable
		response = requests.post(url, headers=headers, data=viz_data)
		response.raise_for_status()
		data_uri = response.text
		return(data_uri)

	# If error occurs with API call, print error and quit script
	except requests.exceptions.RequestException as err:
		print(err)
		print(err.response.text)
		sys.exit()

apiCounter = 1
index = 0

for chunkIndex in range(0,len(startTimeChunk)):
	thisStartTimeEpoch = startTimeChunk[chunkIndex]
	thisEndTimeEpoch = endTimeChunk[chunkIndex]
	
	# Call functions
	this_json_chart_data = prep_json_data(int(thisStartTimeEpoch),int(thisEndTimeEpoch),json_chart_data)
	if apiCounter > 30:
		if time.time() - lastApiTimeStamp < 2.0:
			time.sleep(2.0 - (time.time() - lastApiTimeStamp)) 			# Prevent getting throttled by API
	lastApiTimeStamp = time.time()
	print ('Performing api call number '+str(apiCounter)+'.')
	kentik_data = data_api_call(email, api, this_json_chart_data)
	apiCounter += 1
	thisList = json.loads(kentik_data)

	for dataset in thisList['results'][0]['data']:	
		if 'timeSeries' in dataset.keys():
			cleanKey = dataset['key'].replace(' ---- ','|').replace(',',' ')
			found = False
			try: 
				len(usageData)
			except NameError:
					usageData = [[]]
					usageData[0] = [cleanKey,0,0,0,0]
					index = 0
			else:
				for n in range(0,len(usageData)):   #find the LAST instance of this key (allowing for multiple instances)
					if cleanKey in usageData[n]:
						found = True
						index = n
				if not found:
					usageData.append([cleanKey,0,0,0,0])
					index = len(usageData)-1

			for jsonData in dataset['timeSeries']['both_bits_per_sec']['flow']:
				timeStamp = int(jsonData[0]/1000) # we don't need milliseconds
				duration = int(jsonData[2])
				kB = int(((jsonData[1]/8)/1000)*duration) # convert to kilobytes
				if kB > 0:	
					if usageData[index][1] == 0: # most likely a new key
						usageData[index][1] = timeStamp
					if usageData[index][2] > 0:  #newly found flow data on existing key
						usageData.append([cleanKey,timeStamp,0,timeStamp,0.0])
						index = len(usageData)-1
					usageData[index][3] = timeStamp
					usageData[index][4] = usageData[index][4] + kB
				if kB == 0 and usageData[index][1] > 0 and timeStamp - usageData[index][3] > maxIdleTime:			# we found usage data, but it stopped.
					usageData[index][2] = usageData[index][3]+duration	

print ('Exporting data to '+str(args.output_file)+'.')
if sortBy == 'key':
	usageData = sorted(usageData, key=itemgetter(1))
	usageData = sorted(usageData, key=itemgetter(0))
if sortBy == 'start':
	usageData = sorted(usageData, key=itemgetter(0))
	usageData = sorted(usageData, key=itemgetter(1))
with open(args.output_file, "w") as outfile:
	for n  in range(0,len(usageData)):
		if int(usageData[n][2]) == 0:
			usageData[n][2] = int(endTimeEpoch)
		print(usageData[n][0]+','+str(datetime.datetime.fromtimestamp(int(usageData[n][1])))+','+str(datetime.datetime.fromtimestamp(int(usageData[n][2])))+','+str(int(usageData[n][4])), file=outfile)

