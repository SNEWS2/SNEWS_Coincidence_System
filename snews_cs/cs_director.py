#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on @date 

@author: mlinvill
"""
from socket import gethostname
from typing import List
from time import sleep
import multiprocessing as mp
from multiprocessing import Value, Pipe
import os

import hop
from rich.console import Console

from distributed.lock import DistributedLock
from snews_cs.snews_coinc import CoincidenceDistributor
from snews_cs.alert_pub import AlertListener


def runlock(state: mp.Value, me: str, peers: List):
    dl = DistributedLock(me, peers)
    dl.run(state)

def runlistener(env_path: str, use_local_db: bool, firedrill_mode: bool) -> None:
    al = AlertListener(env_path=env_path, use_local=use_local_db, firedrill_mode=firedrill_mode)
    al.run()

def runcoincidence(leader: Value) -> None:
    server_tag = gethostname() + "_dev"
    coinc = CoincidenceDistributor(leader, use_local_db=False,
                                   drop_db=True,
                                   firedrill_mode=False,
                                   server_tag=server_tag,
                                   send_email=False,
                                   send_slack=False)

    return coinc.run_coincidence()


class Director:
    """
    Coordinate the multiple process error handling and propagation as well as state between the
    multiple processes in the SNEWS_CS server.
    """
    def __init__(self, dl_enable: bool, dl_endpoint: str, dl_peers: List):
        """

        """
        self.distributedlock = dl_enable
        self.dl_endpoint = dl_endpoint
        self.dl_peerenv = dl_peers

        self.env_path = "./auxiliary/test-config.env"

        # mp.set_start_method('spawn')
        self.localcomm, self.remotecomm = mp.Pipe()
        self.leader = mp.Value('i', 0, lock=True)

        self.coincidenceproc = mp.Process(target=runcoincidence, args=(self.leader,))
        if self.distributedlock:
            self.distributedlockproc = mp.Process(target=runlock, args=(self.leader, self.dl_me, self.dl_peers))
        else:
            self.leader = True

        self.listenproc = mp.Process(target=runlistener, args=(self.env_path, False, False))

    def run(self):
        print(f'hop version: {hop.__version__}')

        try:
            print("Starting distributedlock...")
            self.distributedlockproc.start()

            print("Starting AlertListener...")
            self.listenproc.start()

            print("Starting CoincidenceDistributor...")
            self.coincidenceproc.start()

        except KeyboardInterrupt:
            print("Caught a keyboard interrupt.  Goodbye world!")
            log.error(f"(2) Caught a keyboard interrupt. Exiting.\n")

            """ 
            Propagate this error to the other processes!
            """

            self.coincidenceproc.join()
            print("CoincidenceDistributor ended.")
            if self.distributedlock:
                self.distributedlockproc.join()
                print("distributedlock ended.")

            self.listenproc.join()
            print("AlertListener ended.")
            sys.exit(1)

        finally:
            self.coincidenceproc.join()
            print("CoincidenceDistributor ended.")
            if self.distributedlock:
                self.distributedlockproc.join()
                print("distributedlock ended.")

            self.listenproc.join()
            print("AlertListener ended.")
