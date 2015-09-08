import datetime

epoch = datetime.datetime.utcfromtimestamp(0)


def time_in_s():
    delta = datetime.datetime.now() - epoch
    return delta.total_seconds()


def time_in_ms():
    return int(time_in_s() * 1000.0)
