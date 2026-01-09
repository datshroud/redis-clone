
import asyncio
import time
from app import storage
import app.debug.debug as debug
from app.protocol.resp import resp_array, resp_bulk, resp_error, resp_nested_array, resp_stream


def parse_range_id(id, is_start, key = None):
    if id == "$":
        if key not in storage.stream_mem:
            return 0, 0
        id = storage.stream_mem[key][-1][0]
    if id == "-" or id == "+":
        return (0, 0) if is_start else (float("inf"), float("inf"))
    if '-' in id:
        ms, seq = id.split('-', 1)
        try:
            return int(ms), int(seq)
        except ValueError:
            return None
    else:
        try:
            return int(id), 0 if is_start else float("inf")
        except ValueError:
            return None


def current_time_ms():
    return int(time.time() * 1000)

def parse_stream_id(id):
    if id == "*":
        return None, None
    if '-' not in id:
        return None
    ms, seq = id.split('-', 1)
    if seq == "*":
        try:
            return int(ms), None
        except ValueError:
            return None
    try:
        return int(ms), int(seq)
    except ValueError:
        return None
    
def validate_stream_id(key, id):
    ms, seq = id
    if ms == 0 and seq == 0:
        return "ERR the ID specified in XADD must be greater than 0-0"
    stream = storage.stream_mem[key]
    if not stream:
        return None
    last_id, _ = stream[-1]
    last_ms, last_seq = parse_stream_id(last_id)

    if ms < last_ms:
        return "ERR The ID specified in XADD is equal or smaller than the " \
		"target stream top item"
    if ms == last_ms and seq <= last_seq:
        return "ERR The ID specified in XADD is equal or" \
		" smaller than the target stream top item"
    return None

def generate_sequence(key, ms):
    stream = storage.stream_mem.setdefault(key, [])
    if not stream:
        return 1 if ms == 0 else 0
    last_id, _ = stream[-1]
    last_ms, last_seq = parse_stream_id(last_id)
    return last_seq + 1 if last_ms == ms else 0

def generate_full_id(key):
    ms = current_time_ms()
    stream = storage.stream_mem.setdefault(key, [])
    if not stream:
        return ms, (0 if ms > 0 else 1)
    last_id, _ = storage.stream_mem[key][-1]
    last_ms, last_seq = parse_stream_id(last_id)
    return ms, (last_seq + 1 if last_ms == ms else 0)

def query_single_stream(key, id):
    parsed = parse_range_id(id, False, key)
    if not parsed:
        return None
    last_ms, last_seq = parsed
    debug.log(f"last_ms = {last_ms}, last_seq = {last_seq}")
    if last_seq is None:
        last_seq = float("inf")
        if last_ms is None:
            last_ms = float("inf")
    if key not in storage.stream_mem:
        return [key, []]
    stream = storage.stream_mem[key]
    res = [key, []]
    for id_str, data in stream:
        ms, seq = parse_stream_id(id_str)
        if ms < last_ms:
            continue
        if ms == last_ms and seq <= last_seq:
            continue
        res[1].append([id_str, data])
    return res


async def cmd_xadd(command):
	if len(command) < 5 or len(command) % 2 == 0:
		return resp_error("ERR wrong number of arguments for 'xadd' command")
	key = command[1]
	id = command[2]
	if len(command) % 2 == 0:
		return resp_error("ERR wrong number of args for XADD")
	parsed = parse_stream_id(id)
	if not parsed:
		return resp_error("ERR invalid stream ID")
	if key in storage.kv_mem or key in storage.list_mem:
		return resp_error("ERR wrong type")
	if key not in storage.stream_mem:
		storage.stream_mem[key] = []
	ms, seq = parsed
	if ms is None and seq is None:
		ms, seq = generate_full_id(key)
	elif seq is None:
		seq = generate_sequence(key, ms)
	err = validate_stream_id(key, (ms, seq))
	if err:
		return resp_error(err)
	# print("ms, seq = ", ms, seq)
	id = f"{ms}-{seq}"
	data = {}
	for i in range(3, len(command), 2):
		data[command[i]] = command[i + 1]
	storage.stream_mem[key].append((id, data))
	storage.notify_stream_add(key)
	return resp_bulk(id)


async def cmd_xrange(command):
	if len(command) != 4:
		return resp_error("ERR wrong number of arguments for 'xrange' command")
	key = command[1]
	if key not in storage.stream_mem:
		return resp_array([])
	try:
		start = parse_range_id(command[2], True)
		end = parse_range_id(command[3], False)
	except ValueError:
		resp_error("ERR invalid stream ID")
	
	stream = storage.stream_mem[key]
	res = []
	for id_str, data in stream:
		parsed_value = parse_stream_id(id_str)
		if start <= parsed_value <= end:
			res.append((id_str, data))
	# debug.log(f"XRANGE {key} from {start} to {end}, result={len(res)}")
	return resp_stream(res)


async def cmd_xread(command):
	if len(command) < 4:
		return resp_error("ERR wrong number of arguments for 'xread' command")
	if command[1].upper() == "BLOCK":
		if len(command) != 6:
			return resp_error("ERR syntax error")
		try:
			timeout = float(command[2]) / 1000
		except ValueError:
			return resp_error("ERR value is not a float")
		if command[3].upper() != "STREAMS":
			return resp_error("ERR syntax error")
		key = command[4]
		last_id = command[5]
		fut = asyncio.get_event_loop().create_future()
		res = query_single_stream(key, last_id)
		if res is None:
			res = []
		if res[1]:
			return resp_nested_array([res])
		parsed = parse_range_id(last_id, False, key)
		last_id = f"{parsed[0]}-{parsed[1]}"
		storage.stream_waiter_mem[key].append((fut, last_id))
		try:
			if timeout > 0:
				await asyncio.wait_for(fut, timeout)
			elif timeout == 0:
				await fut
			else:
				return resp_error("ERR cant block in negative milisecond")
		except asyncio.TimeoutError:
			storage.remove_fut_stream(key, fut)
			return resp_array()
		res = query_single_stream(key, last_id)
		if res is None:
			res = []
		if res[1]:
			return resp_nested_array([res])
		
	else:
		if command[1].upper() != "STREAMS":
			return resp_error("ERR syntax error")
		if len(command) % 2 or len(command) < 4:
			return resp_error("ERR syntax error")
		idx = 2
		diff = (len(command) - 2) // 2
		res = []
		while idx + diff < len(command):
			key = command[idx]
			id = command[idx + diff]
			debug.log(f"key = {key}, id = {id}")
			query = query_single_stream(key, id)
			if query is None:
				return resp_error("ERR invalid stream ID")
			if query[1]:
				res.append(query)
			idx += 1
		return resp_nested_array(res)
