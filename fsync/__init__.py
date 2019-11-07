from fsync.master import Master
from fsync.slave import Slave
import json
import sys

def print_help_exit():
  usage =\
"""
python -m fsync [config.json]
[config.json] is an optional argument for the path of the configuration file.

Master configuration example: %s
Slave configuration example: %s
""".strip()
  print(usage % ("", ""))
  exit(-1)

if __name__ == '__main__':
  config_file = "config.json"
  if len(sys.argv) > 2:
      print_help_exit()
  elif len(sys.argv) == 2:
    config_file = sys.argv[1]

  config_dict = json.load(open(config_file, encoding="utf-8"))


