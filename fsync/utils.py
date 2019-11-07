import hashlib
import base64
import re
import functools

def sha1(b: bytes, salt: bytes=None):
  s = hashlib.sha1()
  s.update(b)
  if salt is not None:
    s.update(salt)
  return s.digest()

def b64encode(b: bytes):
  return base64.b64encode(b).decode('utf-8')

def b64decode(s: str):
  return base64.b64decode(s)

@functools.lru_cache()
def re_from_wildcard(wildcard: str):
  exp = [""]

  for c in wildcard:
    if c == "*":
      exp.append(".*")
    elif c == "?":
      exp.append(".")
    else:
      exp.append(c)

  return "".join(exp)

@functools.lru_cache()
def match(wildcard: str, s: str):
  regex = re_from_wildcard(wildcard)
  return re.fullmatch(regex, s) is not None

def configure_socks(version, address, port):
  import socks
  import socket
  socks.set_default_proxy(socks.PROXY_TYPES[version], address, port)
  socket.socket = socks.socksocket