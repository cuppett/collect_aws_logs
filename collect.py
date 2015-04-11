# Copyright (c) 2015 Stephen Cuppett <stephen.cuppett@sas.com>
# All rights reserved

from boto import logs
from boto.logs.exceptions import LimitExceededException
import tempfile
import time
import os
import shutil

# Important variables which control log group collection
cwlogs = logs.connect_to_region('us-east-1')
log_prefix = 'Green'
parent_path = tempfile.gettempdir()
minutes_back = 20

# Computing how much log data to bring back.
startTime = int(time.time() * 1000) - (minutes_back * 60 * 1000)

# Retrieving initial list of groups to use.
group_response = cwlogs.describe_log_groups(log_prefix, None, None)
has_more_groups = True

def getLogEvents(logGroup, logStream, startTime, nextForwardToken = None):
    """
    Handles fetching the next batch of log events. Will account for rate limits
    and continue trying until it can get through.
    """
    log_events = None
    while not log_events:
        try:
            log_events = cwlogs.get_log_events(logGroup, logStream, startTime, None, nextForwardToken, None, True)
        except LimitExceededException:
            log_events = None
            time.sleep(1)
    return log_events

# Loop over initial group response
while has_more_groups:
    has_more_groups = False    
    
    # Loop over individual groups
    for logGroup in group_response['logGroups']:
        
        # Ascertain directory to create log files in.
        directory = parent_path + os.path.sep + logGroup['logGroupName']
        if os.path.exists(directory):
            print('Removing existing contents in ' + directory)
            shutil.rmtree(directory)
        os.mkdir(directory)
        
        # Get the individual streams.
        streams_response = cwlogs.describe_log_streams(logGroup['logGroupName'], None, None)
        has_more_streams = True
        while has_more_streams:
            has_more_streams = False
            
            # Work our way through the streams
            for stream in streams_response['logStreams']:
                # Create the file.
                file = directory + os.path.sep + stream['logStreamName'] + '.log'
                log_file = None
                log_events = None

                more_log_events = stream['storedBytes'] > 0 and stream['lastEventTimestamp'] > startTime

                if more_log_events:
                    log_events = getLogEvents(logGroup['logGroupName'], stream['logStreamName'], startTime)
                    print('Creating log file ' + file)
                    log_file = open(file, 'w')
                                
                while more_log_events:

                    for log_line in log_events['events']:
                        try:
                            # If we have seen the last message, there's no more to process
                            if stream['lastEventTimestamp'] == log_line['timestamp']:
                                more_log_events = False
                            log_file.write(log_line['message'])
                        except:
                            log_file.write('Bad line read from logs.')
                
                    # When the forward and backward tokens match, there are no more pages to fetch.
                    if more_log_events:
                        more_log_events = log_events['nextForwardToken'][2:] != log_events['nextBackwardToken'][2:]
                
                    if more_log_events:
                        log_events = getLogEvents(logGroup['logGroupName'], stream['logStreamName'], startTime, log_events['nextBackwardToken'])
            
                if log_file:
                    log_file.close()
            
            if 'nextToken' in streams_response:
                streams_response = cwlogs.describe_log_streams(logGroup['logGroupName'], streams_response['nextToken'], None)
                has_more_streams = True
        
    if 'nextToken' in group_response:
        group_response = cwlogs.describe_log_groups(log_prefix, group_response['nextToken'], None)
        has_more_groups = True
    
print()
print(' - For Steven')
