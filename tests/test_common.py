import pytest
from fsync.common import FileInfo, MappingConfig, MasterMessage, SlaveMessage,\
  get_dir_fileinfo
from fsync import utils

def _test_serialize_deserialize(o):
  assert o.deserialize(o.serialize()) == o

def test_mapping_config():
  cfg1 = MappingConfig()
  s1 = cfg1.serialize()
  print(len(s1), s1)

  _test_serialize_deserialize(cfg1)

def test_message():
  m = MasterMessage(filename="shit", content=b"fuck", type=1)
  _test_serialize_deserialize(m)

  m = SlaveMessage(filename="shit", hash=b"fuck", type=1)
  _test_serialize_deserialize(m)

def test_get_dir_fileinfo():
  assert len(get_dir_fileinfo("./data/",
                              excluding_wildcards=["*.idea*"])) == 7

def test_run_get_dir_fileinfo():
  print("\n".join(str(i) for i in get_dir_fileinfo("./data/", excluding_wildcards=["*.idea*"])))