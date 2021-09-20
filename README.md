# k_timer
Python script to pull data from API and output start/end times of data transfers.
Transfers are grouped by a key, which is a combination of dimensions selected in Data Explorer.
This will output the key,start time,end time,total transfer in kilobytes as a csv.file
```
commandline$ ./k_timer.py -h
usage: k_timer.py [-h] [-e] [-a] [-if] [-st] [-et] [-idle] [-sort] output_file

k_timer.py: a cli utility to retrieve a top talkers (up to 350) and export start and end times of transactions
- Output filename required at command line
- Input file should be a copy of JSON Query as found in Kentik Data Explorer.
- Kentik email address & API token required at command line.
- If start and end times are not included in the command line, (-st and -et) then the start/end from json input query are used.
- Max Idle Time is the maximum amount of time (in seconds)that a flow may be dormant before consided to end.  (for "bouncy" flows)
- Email and API info can be found in your Kentik profile (https://portal.kentik.com/profile4/info)

positional arguments:
  output_file           Name of output file

optional arguments:
  -h, --help            show this help message and exit
  -e , --email          Kentik User Email
  -a , --api            API key for User
  -if , --input_file    Filename [input.json]
  -st , --start_time    Start time for query [json query file]
  -et , --end_time      End time for query [json query file]
  -idle , --max_idle_time
                        Maximum idle time per transfer, in seconds [60]
  -sort , --sort_field
                        What to sort output by.  May be "key" or "start" [start]
```
