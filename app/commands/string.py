from __future__ import annotations

import time
from typing import List

import app.storage as storage
from app.protocol.resp import resp_bulk, resp_error, resp_int, resp_simple


async def cmd_ping(command):
	return resp_simple("PONG")


async def cmd_echo(command):
	if len(command) != 2:
		return resp_error("ERR wrong number of arguments for 'echo' command")
	return resp_bulk(command[1])


async def cmd_set(command):
	key = command[1]
	val = command[2]
	if len(command) == 5:
		opt = command[3].upper()
		if opt != "EX" and opt != "PX":
			return resp_error(f"What is {opt}? We only have EX and PX.")
		t = int(command[4])
		sec = t if opt == "EX" else t / 1000
		expire_time = time.time() + sec
		storage.expire_mem[key] = expire_time
	else:
		storage.expire_mem.pop(key, None)
	storage.kv_mem[key] = val
	return resp_simple("OK")


async def cmd_get(command):
	if len(command) != 2:
		return resp_error("ERR wrong number of arguments for 'get' command")
	key = command[1]
	if key not in storage.kv_mem or storage.is_expired(key):
		return resp_bulk(None)
	return resp_bulk(storage.kv_mem[key])


async def cmd_incr(command):
	if len(command) != 2:
		return resp_error("ERR wrong number of arguments for 'incr' command")
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
