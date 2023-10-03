from Distributed.Lock import DistributedLock
from snews_cs.snews_coinc import CoincidenceDistributor
import hop
from socket import gethostname
from typing import List
from time import sleep
from rich.console import Console

import multiprocessing as mp

# from  snews_cs import  _version

def runlock(state: mp.Value, me: str, peers: List):
    dl = DistributedLock(me, peers)
    dl.run(state)


if __name__ == '__main__':
    print(f'hop version: {hop.__version__}')

    # XXX - Need to load the env before here.
    me = os.getenv('DISTRIBUTED_LOCK_ENDPOINT')
    peers = os.getenv('DISTRIBUTED_LOCK_PEERS')

    # print(f'SNEWS CS version: {_version.__version__}')
    server_tag = gethostname()
    coinc = CoincidenceDistributor(use_local_db=True,
                                   drop_db=True,
                                   firedrill_mode=False,
                                   server_tag=server_tag,
                                   send_email=True)

    mp.set_start_method('spawn')
    leader = mp.Value('i', 0, lock=True)

    coincidenceproc = mp.Process(target=coinc.run_coincidence, args=leader)
    distributedlockproc = mp.Process(target=runlock, args=(leader, me, peers))
    listenproc = mp.Process(target=coinc.run_alert_listener)

    listenproc.start()
    distributedlockproc.start()
    coincidenceproc.start()

    coincidenceproc.join()
    distributedlockproc.join()
    listenproc.join()