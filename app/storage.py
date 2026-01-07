from collections import defaultdict
import time

import app.debug as debug

class ListNode:
    __slots__ = ("value", "prev", "next")

    def __init__(self, value, prev = None, next = None):
        self.value = value
        self.prev = prev
        self.next = next
class LinkedList:
    __slots__ = ("_head", "_tail", "_size")

    def __init__(self):
        self._head = None
        self._tail = None
        self._size = 0

    def append(self, waiter):
        self._size += 1
        node = ListNode(waiter)
        if not self._head:
            self._head = self._tail = node
            return
        self._tail.next = node
        self._tail = node
        

    def remove(self, node: ListNode):
        if self._size == 0:
            return
        prev = node.prev
        next = node.next
        if prev:
            prev.next = next
        else:
            self._head = next
        if next:
            next.prev = prev
        else:
            self._tail = prev
        node.next = node.prev = None
        self._size -= 1
        
    def get_list(self):
        current_node = self._head
        res = []
        while current_node:
            res.append(current_node)
            current_node = current_node.next
        return res
    
kv_mem = {}
expire_mem = {}
list_mem = {}
waiter_mem = {}
stream_mem = {}
stream_waiter_mem = defaultdict(LinkedList)


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

def notify_stream_add(key):
    if key in stream_waiter_mem:
        stream_waiter_list = stream_waiter_mem[key].get_list()
        id = stream_mem[key][-1][0]
        ms, seq = id.split('-')
        ms = int(ms)
        seq = int(seq)
        for stream_waiter in stream_waiter_list:
            fut, last_id = stream_waiter.value
            debug.log(f"last_id = {last_id} (in notify func)")
            last_ms, last_seq = last_id.split('-')
            last_ms = int(last_ms)
            last_seq = int(last_seq)
            # debug.log(f"""last_ms = {last_ms}, last_seq = {last_seq},
            #     ms = {ms}, seq = {seq}""")
            if (ms, seq) > (last_ms, last_seq):
                stream_waiter_mem[key].remove(stream_waiter)
                if not fut.done():
                    fut.set_result(True)

def remove_fut_stream(key, fut_target):
    stream_waiter_list = stream_waiter_mem[key].get_list()
    for stream_waiter in stream_waiter_list:
        fut, last_id = stream_waiter.value
        if fut == fut_target:
            stream_waiter_mem[key].remove(stream_waiter)
            return

