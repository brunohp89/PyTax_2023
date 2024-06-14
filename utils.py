import pickle as pk
import datetime as dt

def save_pickle(obj, name):
    with open(name, "wb") as handle:
        pk.dump(obj, handle, protocol=pk.HIGHEST_PROTOCOL)


def open_pickle(name):
    with open(name, "rb") as handle:
        vout = pk.load(handle)
    return vout


def date_from_timestamp(x):
    time_now_sys = dt.datetime.strptime(dt.datetime.now(dt.datetime.now().astimezone().tzinfo).strftime('%Y-%m-%d '
                                                                                                        '%H:%M:%S'),
                                        '%Y-%m-%d %H:%M:%S')
    time_now_utc = dt.datetime.strptime(dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                                        '%Y-%m-%d %H:%M:%S')
    diff_utc = -round(((time_now_sys - time_now_utc).seconds / 60) / 60)
    return dt.datetime.fromtimestamp(int(x)) + dt.timedelta(hours=diff_utc)
