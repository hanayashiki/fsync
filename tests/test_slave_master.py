import threading
from fsync.master import Master
from fsync.slave import Slave
from fsync import common
import asyncio
import os
import shutil
import time
import logging

def run_thread(runnable: common.Runnable):
  def run():
    asyncio.run(runnable.run())
  thread = threading.Thread(target=run)
  thread.daemon = True
  thread.start()

  return thread

def test_slave_master():
  slave_dir = "tmp/slave_dir"
  os.makedirs(slave_dir, exist_ok=True)
  try:
    cfg = common.MappingConfig(master_dir="data/", slave_dir=slave_dir)

    master = Master(slave_addr=("127.0.0.1", 31415),
                    ssl_context=None,
                    mapping_config=cfg)

    slave = Slave(listen_addr=("127.0.0.1", 31415),
                  ssl_context=None)

    process_slave = run_thread(slave)
    time.sleep(0.5)
    process_master = run_thread(master)

    time.sleep(10)
    return

  finally:
    pass
    # shutil.rmtree("tmp")
