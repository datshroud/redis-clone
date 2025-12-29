import time

kv_mem = {}
expire_mem = {}
list_mem = {}

def is_expired(key):
    if key in expire_mem and time.time() >= expire_mem[key]:
        kv_mem.pop(key, None)
        expire_mem.pop(key, None)
        list_mem.pop(key, None)
        return True
    return False