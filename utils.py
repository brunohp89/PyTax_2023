import pickle as pk


def save_pickle(obj, name):
    with open(name, "wb") as handle:
        pk.dump(obj, handle, protocol=pk.HIGHEST_PROTOCOL)


def open_pickle(name):
    with open(name, "rb") as handle:
        vout = pk.load(handle)
    return vout
