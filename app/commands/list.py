

import asyncio
from app import storage
from app.protocol.resp import resp_array, resp_bulk, resp_error, resp_int


async def cmd_rpush(command):
	if len(command) < 3:
		return resp_error("ERR invalid RPUSH command")
	key = command[1]
	if key in storage.kv_mem:
		return resp_error("ERR int cannot append like list")
	if key not in storage.list_mem:
		storage.list_mem[key] = []
	storage.list_mem[key] += command[2:]
	storage.notify_list_push(key)
	return resp_int(len(storage.list_mem[key]))

async def cmd_lpush(command):
	if len(command) < 3:
		return resp_error("ERR wrong number of arguments for 'lpush' command")
	arr = command[2:]
	key = command[1]
	if key not in storage.list_mem:
		storage.list_mem[key] = []
	storage.list_mem[key] = arr[::-1] + storage.list_mem[key]
	for i in range(2, len(command)):
		storage.notify_list_push(key)
	return resp_int(len(storage.list_mem[key]))


async def cmd_lrange(command):
	if len(command) != 4:
		return resp_error("ERR wrong number of arguments for 'lrange' command")
	key = command[1]
	if key in storage.kv_mem:
		return resp_error("ERR int cannot range like list")
	if key not in storage.list_mem:
		return resp_array()
	arr = storage.list_mem[key]
	try:
		narr = len(arr)
		start = int(command[2])
		end = int(command[3])
		if start < 0:
			start += narr
		if end < 0:
			end += narr + 1
		start = max(0, min(narr, start))
		end = max(0, min(end, narr))
	except ValueError:
		return resp_error("ERR value is not an integer")
	return resp_array(arr[start : end])


async def cmd_llen(command):
	if len(command) != 2:
		return resp_error("ERR wrong number of arguments for 'llen' command")
	key = command[1]
	if key not in storage.list_mem:
		return resp_int(0)
	return resp_int(len(storage.list_mem[key]))


async def cmd_lpop(command):
	if len(command) not in (2, 3):
		return resp_error("ERR wrong number of arguments for 'lpop' command")
	key = command[1]
	start = 1
	if len(command) == 3:
		try:
			start = max(0, int(command[2]))
		except ValueError:
			return resp_error("ERR value is not an integer")
	if key not in storage.list_mem:
		return resp_bulk(None)
	pop_arr = storage.list_mem[key][:start]
	storage.list_mem[key] = storage.list_mem[key][start:]
	return resp_array(pop_arr)


async def cmd_blpop(command):
	if len(command) < 3:
		return resp_error("ERR wrong number of arguments for 'blpop' command")
	key = command[1]
	try:
		timeout = float(command[2])
	except ValueError:
		return resp_error("ERR value is not a float")
	
	if key in storage.list_mem and storage.list_mem[key]:
		val = storage.list_mem[key].pop(0)
		return resp_array([key, val])
	
	fut = asyncio.get_event_loop().create_future()
	storage.waiter_mem.setdefault(key, []).append(fut)

	try:
		if timeout > 0:
			await asyncio.wait_for(fut, timeout)
		else:
			await fut
	except asyncio.TimeoutError:
		storage.waiter_mem[key].remove(fut)
		return resp_array()
	
	if key in storage.list_mem and storage.list_mem[key]:
		return resp_array([key, storage.list_mem[key].pop(0)])
	return resp_array()
