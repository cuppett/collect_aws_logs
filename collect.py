from boto import logs
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
                log_file = open(file, 'w')
                print('Creating log file ' + file)

                more_log_events = False
                try:
                    # Pulling in the log events.
                    log_events = cwlogs.get_log_events(logGroup['logGroupName'], stream['logStreamName'], startTime, None, None, None, True)
                    more_log_events = True
                    count = 1
                except:
                    print('Failure pulling log events.')
                                
                while more_log_events and count < 5:
                    more_log_events = False
                    count = count + 1
                    
                    for log_line in log_events['events']:
                        try:
                            log_file.write(log_line['message'])
                        except:
                                log_file.write('Bad line read from logs.')
                
                    if 'nextForwardToken' in log_events:
                        try:
                            time.sleep(2)
                            log_events = cwlogs.get_log_events(logGroup['logGroupName'], stream['logStreamName'], startTime, None, log_events['nextForwardToken'], None, True)
                            more_log_events = True
                        except:
                            print('Failure pulling log events.')
                            more_log_events = False
            
                log_file.close()
            
            if 'nextToken' in streams_response:
                time.sleep(2)
                streams_response = cwlogs.describe_log_streams(logGroup['logGroupName'], streams_response['nextToken'], None)
                has_more_streams = True
        
    if 'nextToken' in group_response:
        group_response = cwlogs.describe_log_groups(log_prefix, group_response['nextToken'], None)
        has_more_groups = True
    
print()
print(' - For Steven')
