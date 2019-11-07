import asyncio
from typing import *
import ssl
import websockets
import logging
import traceback
from dataclasses import dataclass
import os
import re
import traceback
import click
import json

from fsync.common import FileInfo, SlaveConfig, MappingConfig, ConfigurationError, SlaveMessage, MasterMessage
from fsync import common

logger = logging.getLogger("fsync")

@dataclass
class Context:
  mapping_config: MappingConfig
  file_hashes: Dict[str, bytes]

class Slave(common.Runnable):

  def __init__(self,
               listen_addr: Tuple[str, int],
               ssl_context: Optional[ssl.SSLContext],
               allowed_secrets: List[str]):
    self.listen_addr = listen_addr
    self.listen_ip, self.listen_port = listen_addr
    self.ssl_context = ssl_context
    self.allowed_secrets = allowed_secrets

  def normalize_slave_path(self, context: Context, s: str):
    p = s.replace(context.mapping_config.slave_dir.replace("\\", "/") + "/", "", 1)  # Chop off the master_dir
    return p

  def parse_path(self, p: str):
    if p.endswith("/"):
      p = p[0:-1]
    matches = re.findall(r'/(.*)/(.*)', p)
    if len(matches) == 1:
      name, param = matches[0]
      return name, param
    else:
      return None

  async def scan_files(self, context: Context, ws: websockets.WebSocketServerProtocol):
    mapping_config = context.mapping_config
    try:
      while True:
        fileinfos = common.get_dir_fileinfo(mapping_config.slave_dir,
                                            load_content=False,
                                            max_size=mapping_config.max_size,
                                            hash_salt=mapping_config.secret.encode("utf-8"),
                                            excluding_wildcards=mapping_config.excluding_wildcards
                                            )
        messages = []
        for fileinfo in fileinfos:
          if mapping_config.slave_dir != ".":
            normalized_path = self.normalize_slave_path(context, fileinfo.filename)
          else:
            normalized_path = fileinfo.filename

          if normalized_path not in context.file_hashes or context.file_hashes[normalized_path] != fileinfo.hash:
            slave_massage = SlaveMessage(normalized_path, fileinfo.hash, common.UPDATE)
            messages.append(slave_massage.serialize())
            context.file_hashes[normalized_path] = fileinfo.hash

        await ws.send(common.MESSAGE_SEP.join(messages) + common.MESSAGE_SEP)

        await asyncio.sleep(mapping_config.scan_period)
    except Exception as e:
      logger.error("\n" + traceback.format_exc())
      raise
    finally:
      logger.info("quit")


  async def receive_update(self, context: Context, ws: websockets.WebSocketServerProtocol):
    mapping_config = context.mapping_config

    try:
      while True:
        messages = await ws.recv()
        master_messages: List[MasterMessage] = common.split_message(messages, MasterMessage)
        logger.info(f"Receive {len(master_messages)} master messages")

        for m in master_messages:
          if m.type == common.UPDATE:
            filename, file_content = m.filename, m.content
            file_path = os.path.join(mapping_config.slave_dir, filename)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
              f.write(file_content)
              f.flush()
            logger.info(f"Done writing to {file_path}")
          elif m.type == common.DELETE:
            filename = m.filename
            file_path = os.path.join(mapping_config.slave_dir, filename)
            os.remove(file_path)
            try:
              os.removedirs(os.path.dirname(file_path))
            except OSError:
              pass

            logger.info(f"Deleted {file_path}")

        await asyncio.sleep(mapping_config.scan_period)
    except Exception as e:
      logger.error("\n" + traceback.format_exc())
      raise
    finally:
      logger.info("quit")

  async def fsync_handler(self, ws: websockets.WebSocketServerProtocol, param: str):
    mapping_config = MappingConfig.deserialize(param)
    context = Context(mapping_config=mapping_config,
                      file_hashes={})

    if mapping_config.secret not in self.allowed_secrets:
      await ws.close(4403, "Forbidden")

    await asyncio.wait([self.scan_files(context, ws), self.receive_update(context, ws)])


  async def handler(self, ws: websockets.WebSocketServerProtocol, path: str):
    logger.info(f"Incoming ws connection: {ws.remote_address}. ")
    path_info = self.parse_path(path)
    if path_info:
      name, param = path_info
      if name == "fsync":
        await self.fsync_handler(ws, param)
    else:
      return await ws.close(4404, "Not Found")

  @classmethod
  def from_config_dict(cls, d: Dict):
    assert d["type"] == "slave"
    del d["type"]
    slave_config = SlaveConfig(**d)

    if slave_config.use_ssl:
      ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    else:
      ssl_context = None

    return Slave(listen_addr=slave_config.slave_addr,
                 ssl_context=ssl_context,
                 allowed_secrets=slave_config.allowed_secrets)

  async def run(self):
    logger.info(f"Slave started. ")
    await websockets.serve(self.handler,
                           self.listen_ip,
                           self.listen_port,
                           ssl=self.ssl_context)
@click.command()
@click.option("--config", "-c", default="slave.json", help="Json configuration for slave. ")
def main(config):
  config_dict = json.load(open(config))
  slave = Slave.from_config_dict(config_dict)

  asyncio.get_event_loop().run_until_complete(slave.run())
  asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
  main()