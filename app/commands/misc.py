from app import storage
from app.protocol.resp import resp_error, resp_int


async def cmd_type(command):
	if len(command) != 2:
		return resp_error("ERR wrong number of arguments for 'type' command")
	key = command[1]
	if key not in storage.kv_mem:
		storage.kv_mem[key] = 0
	val = storage.kv_mem[key]
	try:
		num = int(val)
	except ValueError:
		return resp_error("ERR value is not an integer or out of range")
	num += 1
	storage.kv_mem[key] = str(num)
	return resp_int(num)