# coding=utf-8

import sys
from utils import parse_line, parse_time_stamp, BlockedAttempts
from datetime import datetime, timedelta

import logging
logger = logging.getLogger('myapp')
hdlr = logging.FileHandler('myapp.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.WARNING)

try:
    proces_log_file = sys.argv[0]
    log_file = sys.argv[1]
    hosts_file = sys.argv[2]
    hours_file = sys.argv[3]
    resources_file = sys.argv[4]
    blocked_file = sys.argv[5]

except IndexError:
    # For testing process_log.py standalone and not using args
    proces_log_file = '../src/process_log.py '
    log_file = '../insight_testsuite/temp/log_input/log.txt'
    hosts_file = '../insight_testsuite/temp/log_output/hosts.txt'
    hours_file = '../insight_testsuite/temp/log_output/hours.txt'
    resources_file = '../insight_testsuite/temp/log_output/resources.txt'
    blocked_file = '../insight_testsuite/temp/log_output/blocked.txt'


def update_feature1(data, hosts_count):
    """

    >>> line = 'unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985'
    >>> data = parse_line(line)
    >>> resources_count = {}
    >>> hosts_count = update_feature1(data, resources_count)
    >>> resources_count
    {'unicomp6.unicomp.net': 1}
    """

    if data['host'] in hosts_count:
        hosts_count[data['host']] += 1
    else:
        hosts_count[data['host']] = 1

    return hosts_count


def update_feature2(data, resources_count):
    """

    >>> line = 'unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985'
    >>> data = parse_line(line)
    >>> resources_count = {}
    >>> resources_count = update_feature2(data, resources_count)
    >>> resources_count
    {'/shuttle/countdown/': 1}
    """

    if data['http_resource'] in resources_count:
        resources_count[data['http_resource']] += 1
    else:
        resources_count[data['http_resource']] = 1

    return resources_count


def update_feature3(data, hours_queue, hours_count):
    """

    >>> line = '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] “POST /login HTTP/1.0” 401 1420'
    >>> data = parse_line(line)
    >>> hours_queue = []
    >>> hours_count = {}
    >>> hours_queue, hours_count = update_feature3(data, hours_queue, hours_count)
    >>> line = 'unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985'
    >>> data = parse_line(line)
    >>> update_feature3(data, hours_queue, hours_count)
    (['01/Jul/1995:00:00:01 -0400', '01/Jul/1995:00:00:06 -0400'], {'01/Jul/1995:00:00:01 -0400': 2, '01/Jul/1995:00:00:06 -0400': 1})
    """

    if data['time_stamp'] not in hours_queue:
        hours_queue.append(data['time_stamp'])

    for hour in hours_queue:
        if hour in hours_count:
            ## need to handle multiple copies of timestamp in queue
            hours_count[hour] += 1
        else:
            hours_count[hour] = 1
    # print hours_queue, hours_count

    time_diff = parse_time_stamp(hours_queue[-1]) - parse_time_stamp(hours_queue[0])
    while time_diff > timedelta(hours=1):
        hours_queue.pop(0)
        time_diff = parse_time_stamp(hours_queue[-1]) - parse_time_stamp(hours_queue[0])

    return hours_queue, hours_count


def update_feature4(line, data, blocked_attempts, blocked_list):
    """
    # test for failed login attempt within 20 sec window
    >>> line = '199.72.81.56 - - [01/Jul/1995:00:00:01 -0400] “POST /login HTTP/1.0” 401 1420'
    >>> data = parse_line(line)
    >>> blocked_list = []
    >>> blocked_attempts = BlockedAttempts()
    >>> blocked_attempts.num_failed_logins = 2
    >>> blocked_attempts.first_failed_time = '01/Jul/1995:00:00:01 -0400'
    >>> blocked_attempts, blocked_list = update_feature4(line, data, blocked_attempts, blocked_list)
    >>> blocked_attempts.num_failed_logins, blocked_attempts.first_failed_time
    (3, '01/Jul/1995:00:00:01 -0400')

    # test for failed login attempt outside 20 sec window
    >>> line = '199.72.81.56 - - [01/Jul/1995:00:01:01 -0400] “POST /login HTTP/1.0” 401 1420'
    >>> data = parse_line(line)
    >>> blocked_list = []
    >>> blocked_attempts = BlockedAttempts()
    >>> blocked_attempts.num_failed_logins = 2
    >>> blocked_attempts.first_failed_time = '01/Jul/1995:00:00:01 -0400'
    >>> blocked_attempts, blocked_list = update_feature4(line, data, blocked_attempts, blocked_list)
    >>> blocked_list, blocked_attempts.num_failed_logins, blocked_attempts.first_failed_time
    ([], 1, '01/Jul/1995:00:01:01 -0400')

    # test for passing login attempt
    >>> line = '199.72.81.56 - - [01/Jul/1995:00:01:01 -0400] “POST /login HTTP/1.0” 200 1420'
    >>> data = parse_line(line)
    >>> blocked_list = []
    >>> blocked_attempts = BlockedAttempts()
    >>> blocked_attempts.num_failed_logins = 2
    >>> blocked_attempts.first_failed_time = '01/Jul/1995:00:00:01 -0400'
    >>> blocked_attempts, blocked_list = update_feature4(line, data, blocked_attempts, blocked_list)
    >>> blocked_list, blocked_attempts.num_failed_logins, blocked_attempts.first_failed_time
    ([], 0, None)
    """

    # check if last login time was greater than 20 seconds, if greater, then reset counters
    if blocked_attempts.first_failed_time != None:
        time_diff = parse_time_stamp(data['time_stamp']) - parse_time_stamp(blocked_attempts.first_failed_time)
        if time_diff > timedelta(seconds=20):
            blocked_attempts.num_failed_logins = 0
            blocked_attempts.first_failed_time = None

    if ('/login' in data['http_resource']) and (data['http_reply_code'] == '401'):  # handle failed login
        blocked_attempts.num_failed_logins += 1

        if blocked_attempts.num_failed_logins == 1:
            blocked_attempts.first_failed_time = data['time_stamp']
        elif blocked_attempts.num_failed_logins > 3:
            blocked_list.append(line)

    elif ('/login' in data['http_resource']) and (data['http_reply_code'] == '200'):  # handle passing login

        blocked_attempts.num_failed_logins = 0

    return blocked_attempts, blocked_list




def read_log_file(log_file):

    # initialize data structures
    logger.info('Initializing data structures')
    hosts_count = {} # data struct for feature1, dict containing top hosts
    resources_count = {} # data struct for feature 2, dict containing top resources
    hours_queue = [] # data struct for feature 3, maintains 1 hour worth of log entries
    hours_count = {} # data struct for feature 3, dict containing busiest 10 hours

    blocked_list = [] # data struct for feature 4, list containing all blocked login attempts
    blocked_attempts = BlockedAttempts()

    # iterate through log file and update data structures
    with open(str(log_file)) as file:
        for line in file:
            # parse line
            line = str(line)
            data = parse_line(line)

            logger.info('Updating data structures')
            # update data structures
            # feature 1
            hosts_count = update_feature1(data, hosts_count)

            # feature 2
            resources_count = update_feature2(data, resources_count)

            # feature 3
            hours_queue, hours_count = update_feature3(data, hours_queue, hours_count)

            # feature 4
            blocked_attempts, blocked_list = update_feature4(line, data, blocked_attempts, blocked_list)

    logger.info('Begin writing to output files')
    # write to output files
    # feature 1: save top 10 hosts to hosts.txt
    logger.info('Begin writing to hosts.txt')
    with open(str(hosts_file), 'w') as file:
        host_number = 0
        for host, value in sorted(hosts_count.items(), key=lambda kv: kv[1], reverse=True):
            if host_number < 10:
                file.write(host + ',' + str(value) + '\n')
            else:
                break
            host_number += 1
    logger.info('Completed writing to hosts.txt')

    logger.info('Begin writing to resources.txt')
    # feature 2: save top 10 resources to resources.txt, sort by value in descending order, then by key ascending value
    with open(str(resources_file), 'w') as file:
        resource_number = 0

        for resource, value in sorted(resources_count.items(), key=lambda(k, v): (-v, k)):
            if resource_number < 10:
                file.write(resource + '\n')
            else:
                break
            resource_number += 1
    logger.info('Completed writing to resources.txt')

    logger.info('Begin writing to hours.txt')
    # feature 3: save top 10 hours to hours.txt
    with open(str(hours_file), 'w') as file:
        hour_number = 0
        for hour, value in sorted(hours_count.items(), key=lambda kv: kv[1], reverse=True):
            if hour_number < 10:
                file.write(hour + ',' + str(value) + '\n')
            else:
                break
                hour_number += 1
    logger.info('Completed writing to hours.txt')

    logger.info('Begin writing to blocked.txt')
    # feature 4: save all blocked login attempts to blocked.txt
    with open(str(blocked_file), 'w') as file:
        for row in blocked_list:
            file.write(row)
    logger.info('Begin writing to blocked.txt')



if __name__ == '__main__':
    import doctest
    doctest.testmod()

    logger.info('Started App')
    read_log_file(log_file)
    logger.info('Completed App')
