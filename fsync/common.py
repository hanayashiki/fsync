from dataclasses import dataclass, field
import json
from fsync import utils
import os
import os.path
import re
from typing import *
import urllib.parse
from abc import ABC, abstractmethod
import socks
import logging

OK = 0
UPDATE = 1
DELETE = 2


MESSAGE_SEP = '\n'

logger = logging.getLogger("fsync")
logging.basicConfig(format='%(asctime)s %(filename)16s %(funcName)32s : %(levelname)s  %(message)s')
logger.setLevel(logging.DEBUG)

class ConfigurationError(Exception):
  pass

class Runnable(ABC):

  @abstractmethod
  async def run(self):
    pass

@dataclass
class MappingConfig:
  max_size: int = 2048 * 1024
  excluding_wildcards: List[str] = None
  scan_period: float = 0.5
  secret: str = "dongfangchulegemaozedong"
  master_dir: str = "."
  slave_dir: str = "."

  def serialize(self):
    return urllib.parse.quote(json.dumps(self.__dict__), safe='')

  @classmethod
  def deserialize(cls, s: str):
    return MappingConfig(**json.loads(urllib.parse.unquote(s)))

@dataclass
class MasterConfig:
  slave_addr: Tuple[str, int]
  use_ssl: bool = False
  allow_insecure: bool = False
  mapping_config: MappingConfig = field(default_factory=MappingConfig)
  use_socks5: bool = False
  socks5_ver: str = "SOCKS5"
  socks5_addr: str = "localhost"
  socks5_port: str = 1080

@dataclass
class SlaveConfig:
  slave_addr: Tuple[str, int]
  use_ssl: bool
  allowed_secrets: List[str] = field(default_factory=list)

@dataclass
class MasterMessage:
  filename: str
  content: bytes
  type: int

  def serialize(self):
    return json.dumps({
      "filename": self.filename,
      "content": utils.b64encode(self.content),
      "type": self.type
    })

  @classmethod
  def deserialize(cls, s: str):
    d = json.loads(s)
    return cls(
      filename=d["filename"],
      content=utils.b64decode(d["content"]),
      type=d["type"]
    )

@dataclass
class SlaveMessage:
  filename: str
  hash: bytes
  type: int

  def serialize(self):
    return json.dumps({
      "filename": self.filename,
      "hash": utils.b64encode(self.hash),
      "type": self.type,
    })

  @classmethod
  def deserialize(cls, s: str):
    d = json.loads(s)
    return cls(
      filename=d["filename"],
      hash=utils.b64decode(d["hash"]),
      type=d["type"]
    )

@dataclass
class FileInfo:
  filename: str
  hash: bytes
  content: bytes

  @classmethod
  def from_file(cls,
                filepath: str,
                load_content: bool=False,
                max_size: int=2048 * 1024,
                hash_salt: bytes=None,
                excluding_wildcards: List[str]=None):
    filesize = os.path.getsize(filepath)

    if excluding_wildcards is None:
      excluding_wildcards = []

    if filesize > max_size:
      return None

    for wildcard in excluding_wildcards:
      if utils.match(wildcard, filepath):
        return None

    content = open(filepath, mode="rb").read()
    h = utils.sha1(content, hash_salt)
    return FileInfo(
      filename=normpath(filepath),
      content=content if load_content else None,
      hash=h,
    )

def normpath(p):
  return os.path.normpath(p).replace("\\", "/")

def get_dir_fileinfo(dir: str,
                     *args,
                     **kwargs):
  infos = []
  for root, dirnames, files in os.walk(dir):
    for file in files:
      filepath = os.path.join(root, file)
      info = FileInfo.from_file(filepath, *args, **kwargs)
      if info:
        infos.append(info)
  return infos

def split_message(s: str, t: ClassVar):
  return [t.deserialize(m) for m in s.strip().split(MESSAGE_SEP) if len(m) > 0]