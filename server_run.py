from socket import gethostname
from typing import List
from time import sleep
import multiprocessing as mp

from rich.console import Console
import hop
from distributed.lock import DistributedLock
from distributed import Disco, PeerList

from snews_cs.snews_coinc import CoincidenceDistributor

def runlock(state: mp.Value, me: str, peers: List):
    dl = DistributedLock(me, peers)
    dl.run(state)

def rundisco(peers: List) -> List:
    with Disco(broker=os.getenv("BROKER"), read_topic=os.getenv("DISCO_READ_TOPIC"),
               write_topic=os.getenv("DISCO_WRITE_TOPIC")) as disco:
        print(f"discovery state: {disco.get_peerlist()}")
        peers.append(disco.get_peerlist())

    return peers


if __name__ == '__main__':
    print(f'hop version: {hop.__version__}')
    mp.set_start_method('spawn')
    leader = mp.Value('i', 0, lock=True)

    discoproc = mp.Process(target=rundisco, args=())
    discoproc.start()

    # print(f'SNEWS CS version: {_version.__version__}')
    server_tag = gethostname()
    coinc = CoincidenceDistributor(drop_db=True,
                                   firedrill_mode=False,
                                   server_tag=server_tag,
                                   send_email=True)


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