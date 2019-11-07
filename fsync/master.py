import asyncio
from typing import *
import ssl
import websockets
import logging
import traceback
import click
import json

from fsync.common import FileInfo, MappingConfig, ConfigurationError, SlaveMessage, MasterMessage
from fsync import common, utils

logger = logging.getLogger("fsync")

class Master(common.Runnable):

  def __init__(self,
               slave_addr: Tuple[str, int],
               ssl_context: Optional[ssl.SSLContext],
               mapping_config: MappingConfig):
    self.slave_addr = slave_addr
    self.slave_ip, self.slave_port = self.slave_addr
    self.ssl_context = ssl_context
    self.mapping_config = mapping_config
    self.master_dir = common.normpath(self.mapping_config.master_dir)

    self.remote_file_hashes: Dict[str, bytes] = {}
    self.local_file_infos: Dict[str, FileInfo] = {}

  @classmethod
  def from_config_dict(cls, d: Dict):
    assert d["type"] == "master"
    del d["type"]
    mapping_config = MappingConfig(**d["mapping_config"])
    del d["mapping_config"]
    config = common.MasterConfig(**d)

    if config.use_ssl:
      ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
      if config.allow_insecure:
        ssl_context.verify_mode = ssl.CERT_NONE
    else:
      ssl_context = None

    if config.use_socks5:
      utils.configure_socks(config.socks5_ver, config.socks5_addr, config.socks5_port)

    return cls(slave_addr=d["slave_addr"],
               ssl_context=ssl_context,
               mapping_config=mapping_config)

  def get_uri(self):
    if self.ssl_context:
      proto = "wss"
    else:
      proto = "ws"

    config_string = self.mapping_config.serialize()
    uri = f"{proto}://{self.slave_ip}:{self.slave_port}/fsync/{config_string}"

    if len(uri) > 2000:
      raise ConfigurationError(f"URI is too long. ")

    return uri

  def normalize_master_path(self, s):
    p = s.replace(self.master_dir + "/", "", 1)  # Chop off the master_dir
    return p

  async def scan_files(self, ws):
    while True:
      try:
        fileinfos = common.get_dir_fileinfo(self.mapping_config.master_dir,
                                            load_content=True,
                                            max_size=self.mapping_config.max_size,
                                            hash_salt=self.mapping_config.secret.encode("utf-8"),
                                            excluding_wildcards=self.mapping_config.excluding_wildcards,
                                            )
        new_file_infos = {}

        update_messages: List[Tuple[MasterMessage, Optional[bytes]]] = []

        for i, fileinfo in enumerate(fileinfos):
          if self.master_dir != ".":
            normalized_path = self.normalize_master_path(fileinfo.filename)
          else:
            normalized_path = fileinfo.filename
          new_file_infos[normalized_path] = fileinfo

          if normalized_path not in self.remote_file_hashes or self.remote_file_hashes[normalized_path] != fileinfo.hash:
            update_message = MasterMessage(filename=normalized_path,
                                            content=fileinfo.content,
                                            type=common.UPDATE)
            logger.info(f"Uploading {normalized_path}")
            update_messages.append((update_message, fileinfo.hash))
            self.remote_file_hashes[normalized_path] = fileinfo.hash

        delete_set = set(self.local_file_infos) - set(new_file_infos)
        for filename in delete_set:
          update_message = MasterMessage(filename=filename, content=bytes([]), type=common.DELETE)
          update_messages.append((update_message, None))
          logger.info(f"Deleting {filename}")

        if len(update_messages) > 0:
          await ws.send(common.MESSAGE_SEP.join((u.serialize() for u, _ in update_messages)))

        for u, h in update_messages:
          if h:
            self.remote_file_hashes[u.filename] = h
          elif u.filename in self.remote_file_hashes:
            del self.remote_file_hashes[u.filename]

        self.local_file_infos = new_file_infos
      except Exception as e:
        logger.error(traceback.format_exc())
        break
      finally:
        await asyncio.sleep(self.mapping_config.scan_period)

  async def update(self, ws: websockets.WebSocketClientProtocol):
    messages = await ws.recv()
    slave_messages: List[SlaveMessage] = common.split_message(messages, SlaveMessage)
    update_messages: List[Tuple[MasterMessage, Optional[bytes]]] = []

    for m in slave_messages:
      filename, file_hash = m.filename, m.hash

      old_file_hash = self.remote_file_hashes[filename] if filename in self.remote_file_hashes else None

      if old_file_hash != file_hash:
        if filename in self.local_file_infos:
          update_message = MasterMessage(filename=filename,
                                         content=self.local_file_infos[filename].content,
                                         type=common.UPDATE)
          update_messages.append((update_message, m.hash))
        else:
          update_messsage = MasterMessage(filename=filename,
                                          content=bytes([]),
                                          type=common.DELETE)
          update_messages.append((update_messsage, None))

        self.remote_file_hashes[m.filename] = m.hash


    return update_messages

  async def receive_updates(self, ws: websockets.WebSocketClientProtocol):
    """
    Slave messages are `SlaveMessage` separated by MESSAGE_SEP
    """
    while True:
      try:
        update_messages = await self.update(ws)

        if len(update_messages) > 0:
          await ws.send(common.MESSAGE_SEP.join((u.serialize() for u, _ in update_messages)))

      except Exception as e:
        logger.error(traceback.format_exc())
        break

  async def run(self):
    uri = self.get_uri()
    failure_sleep = 5
    while True:
      try:
        logger.info(f"Connecting to {uri}. ")
        async with websockets.connect(uri, ssl=self.ssl_context) as ws:
          logger.info(f"Connection established. ")
          await self.update(ws)
          done, pending = await asyncio.wait([self.receive_updates(ws), self.scan_files(ws)],
                                              return_when=asyncio.FIRST_COMPLETED)
          for p in pending:
            p.cancel()
      except Exception:
        logger.error(traceback.format_exc())
      finally:
        await asyncio.sleep(failure_sleep)

@click.command()
@click.option("--config", "-c", default="master.json", help="Json configuration for master. ")
def main(config):
  config_dict = json.load(open(config))
  master = Master.from_config_dict(config_dict)
  asyncio.run(master.run())


if __name__ == '__main__':
  main()