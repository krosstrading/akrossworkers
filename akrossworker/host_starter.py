import threading
import time
import sys
import subprocess
import multiprocessing
from datetime import datetime


class ClientRunner(threading.Thread):
    def __init__(self, target):
        threading.Thread.__init__(self)
        self.target = target

    def run(self):
        subprocess.call(self.target, shell=True)
        print(datetime.now(), 'process exited', self.target)


if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')

    clients = []
    target = 'python3 -m '
    subprocess.call('which python3', shell=True)
    clients.append(
        ClientRunner(target + 'akrossworker.cybos.create_db.stream_recorder'))
    clients.append(ClientRunner(target + 'akrossworker.cybos.rest_cache'))

    if len(sys.argv) > 1:
        if sys.argv[1] == 'stream':
            clients[0].start()
        elif sys.argv[1] == 'cache':
            clients[1].start()
        elif sys.argv[1] == 'all':
            for client in clients:
                client.start()

    prev = datetime.now()
    while True:
        now = datetime.now()
        if prev.hour == 7 and now.hour == 8:
            for client in clients:
                client.start()

        prev = datetime.now()
        time.sleep(60)
