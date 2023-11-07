from socket import gethostname
from typing import List
from time import sleep
import multiprocessing as mp

from rich.console import Console
import hop
from distributed.lock import DistributedLock

from snews_cs.snews_coinc import CoincidenceDistributor

def runlock(state: mp.Value, me: str, peers: List):
    dl = DistributedLock(me, peers)
    dl.run(state)


if __name__ == '__main__':
    print(f'hop version: {hop.__version__}')

    # XXX - Need to load the env before here.
    me = os.getenv('DISTRIBUTED_LOCK_ENDPOINT')
    peerenv = os.getenv('DISTRIBUTED_LOCK_PEERS')
    if peerenv is not None:
        peers = peerenv.split(',')

    # print(f'SNEWS CS version: {_version.__version__}')
    server_tag = gethostname()
    coinc = CoincidenceDistributor(use_local_db=True,
                                   drop_db=True,
                                   firedrill_mode=False,
                                   server_tag=server_tag,
                                   send_email=True)

    mp.set_start_method('spawn')
    leader = mp.Value('i', 0, lock=True)

    coincidenceproc = mp.Process(target=coinc.run_coincidence, args=(leader,))

    if distributedlock:
        distributedlockproc = mp.Process(target=runlock, args=(leader, me, peers))
        distributedlockproc.start()
    else:
        leader = True

    listenproc = mp.Process(target=coinc.run_alert_listener)
    listenproc.start()

    coincidenceproc.start()

    coincidenceproc.join()
    if distributedlock:
        distributedlockproc.join()

    listenproc.join()