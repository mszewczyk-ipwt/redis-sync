import redis
import threading

class SyncKeys (threading.Thread):
  def __init__(self, source_connection:redis.Redis, dest_connection:redis.Redis):
    self.__source_connection = source_connection
    self.__dest_connection = dest_connection

    threading.Thread.__init__(self)
    self.shutdown_flag = threading.Event()
    self._running = True
  def run(self):
    print("Starting key sync")
    for source_key in self.__source_connection.scan_iter():
      source_value = self.__source_connection.get(source_key)
      self.__dest_connection.set(source_key, source_value)
    print("Finished key sync")
    # while not self.shutdown_flag.is_set():
    #   for hostname,thread in autofs_threads.items():
    #     line = thread.stdout.readline()
    #     if len(line) > 0:
    #       print(line.rstrip().decode())

def parse_config():
  import configparser
  config = configparser.ConfigParser()
  config.read('config.ini')
  return config

if __name__ == "__main__":
  import json

  from re import match

  config = parse_config()
  
  source_connection = redis.Redis(**config["source"])
  dest_connection = redis.Redis(**config["destination"])

  sync_keys_job = SyncKeys(
    source_connection,
    dest_connection
  )
  sync_keys_job.start()

  with source_connection.monitor() as source_monitor:
    for command in source_monitor.listen():
      if bool(config["sync"].get("command_debug")) == True:
        print(f"source command: {command['command']}")
      for sync_command in json.loads(config["sync"].get("synced_commands")):
        if match(sync_command, command["command"]):
          dest_connection.execute_command(command["command"])
          print(f"executed command: {command['command']}")
  
