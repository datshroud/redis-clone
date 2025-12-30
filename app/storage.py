import time

kv_mem = {}
expire_mem = {}
list_mem = {}
waiter_mem = {}

def is_expired(key):
    if key in expire_mem and time.time() >= expire_mem[key]:
        kv_mem.pop(key, None)
        expire_mem.pop(key, None)
        list_mem.pop(key, None)
        return True
    return False

def notify_list_push(key):
    if key in waiter_mem and waiter_mem[key]:
        fut = waiter_mem[key].pop(0)
        if not fut.done():
            fut.set_result(True)

