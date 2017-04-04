# coding=utf-8

import re
import unicodedata
from datetime import datetime
from datetime import timedelta, tzinfo


class BlockedAttempts:
    def __init__(self):
        self.num_failed_logins = 0
        self.first_failed_time = None

def parse_line(line):
    r"""
    # base case
    >>> line = 'unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985'
    >>> for key,value in parse_line(line).iteritems():
    ...     print str(key) + ': ' + str(value)
    bytes: 3985
    host: unicomp6.unicomp.net
    http_method: GET
    time_stamp_datetime: 1995-07-01 00:00:06-04:00
    http_reply_code: 200
    time_stamp: 01/Jul/1995:00:00:06 -0400
    http_protocol: HTTP/1.0
    request: GET /shuttle/countdown/ HTTP/1.0
    http_resource: /shuttle/countdown/

    # handle lines without http_protocol
    >>> line = '215.145.83.92 - - [01/Jul/1995:00:00:41 -0400] "GET /shuttle/missions/sts-71/movies/movies.html" 200 3089'
    >>> for key,value in parse_line(line).iteritems():
    ...     print str(key) + ': ' + str(value)
    bytes: 3089
    host: 215.145.83.92
    http_method: GET
    time_stamp_datetime: 1995-07-01 00:00:41-04:00
    http_reply_code: 200
    time_stamp: 01/Jul/1995:00:00:41 -0400
    http_protocol: None
    request: GET /shuttle/missions/sts-71/movies/movies.html
    http_resource: /shuttle/missions/sts-71/movies/movies.html
    """
    m = re.search(r'(.*?)[\s-]{2}[\[](.*?)[\]] [\"|\“](.*?)[\"|\”] (.*) (.*)', line)

    # handling of lines without http_protocol
    if len(m.group(3).split(' ')) >= 3:
        http_protocol = m.group(3).split(' ')[2]
    else:
        http_protocol = None

    try:
        return_dict = {
            'host': m.group(1).strip()[:-2],
            'time_stamp': m.group(2),
            'time_stamp_datetime': parse_time_stamp(m.group(2)),
            'request': m.group(3),
            'http_method': m.group(3).split(' ')[0],
            'http_resource':  m.group(3).split(' ')[1],
            'http_protocol':  http_protocol,
            'http_reply_code': m.group(4),
            'bytes': m.group(5)

        }
    except IndexError:
        print line
        raise

    return return_dict


def parse_time_stamp(time_stamp_str):
    """
    code used from: http://stackoverflow.com/questions/1101508/how-to-parse-dates-with-0400-timezone-string-in-python

    >>> time_stamp_str = '01/Jul/1995:00:00:06 -0400'
    >>> parse_time_stamp(time_stamp_str)
    datetime.datetime(1995, 7, 1, 0, 0, 6, tzinfo=FixedOffset(-240))
    """

    naive_date_str, _, offset_str = time_stamp_str.rpartition(' ')
    naive_dt = datetime.strptime(naive_date_str, '%d/%b/%Y:%H:%M:%S')
    offset = int(offset_str[-4:-2]) * 60 + int(offset_str[-2:])
    if offset_str[0] == "-":
        offset = -offset
    dt = naive_dt.replace(tzinfo=FixedOffset(offset))

    return dt


class FixedOffset(tzinfo):
    """Fixed offset in minutes: `time = utc_time + utc_offset`."""
    """Code used from http://stackoverflow.com/questions/1101508/how-to-parse-dates-with-0400-timezone-string-in-python

    """
    def __init__(self, offset):
        self.__offset = timedelta(minutes=offset)
        hours, minutes = divmod(offset, 60)
        #NOTE: the last part is to remind about deprecated POSIX GMT+h timezones
        #  that have the opposite sign in the name;
        #  the corresponding numeric value is not used e.g., no minutes
        self.__name = '<%+03d%02d>%+d' % (hours, minutes, -hours)

    def utcoffset(self, dt=None):
        return self.__offset

    def tzname(self, dt=None):
        return self.__name

    def dst(self, dt=None):
        return timedelta(0)

    def __repr__(self):
        return 'FixedOffset(%d)' % (self.utcoffset().total_seconds() / 60)

if __name__ == '__main__':
    import doctest
    doctest.testmod()