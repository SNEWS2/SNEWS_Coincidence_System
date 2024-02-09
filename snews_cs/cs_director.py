#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on @date 

@author: mlinvill
"""
from abc import ABC, abstractmethod
from socket import gethostname
from typing import List, Callable
from time import sleep
import multiprocessing as mp
from multiprocessing import Value
from multiprocessing.connection import Connection
import sys

import hop

from distributed.lock import DistributedLock
from snews_cs.snews_coinc import CoincidenceDistributor
from snews_cs.alert_pub import AlertListener
from snews_cs.cs_utils import set_env
from .core.logging import getLogger

log = getLogger(__name__)


class RemoteProcessError(Exception):
    """
    Class to encapsulate remote process fatal errors.
    """

    def __init__(self, message: str, expression, process_tag: str):
        """"
        """
        self.process_tag = process_tag
        self.expression = expression
        self.message = message


class RemoteProcessWarning(Warning):
    """
    Encapsulate non-fatal warnings from remote processes.
    """

    def __init__(self, message: str, expression, process_tag: str):
        """
        """
        self.process_tag = process_tag
        self.expression = expression
        self.message = message


class DistributedBase(ABC):
    """
    Scaffolding for distributed system error propagation and state communication.
    """

    def __init__(self):
        self._procs = {}
        self._comms = {}
        self._state = {}
        self._err_hndlr = {}

    def state(self, tag: str) -> int:
        if tag in self._state:
            return self._state[tag].value

    def comm(self, tag: str, direction: str) -> Connection:
        if tag in self._comms:
            if direction in self._comms[tag]:
                return self._comms[tag][direction]

        return None

    def proc(self, tag: str):
        if tag in self._procs:
            return self._procs[tag]
        else:
            return None

    def set_state(self, tag: str, state: int) -> bool:
        if tag in self._state:
            with self._state[tag].get_lock():
                self._state[tag].value = state
        else:
            return False

    """
    The methods below are half-implemented. Need to consider server-side (parent) (send and receive) 
    as well as client-side (child) (send and receive)
    """

    def broadcast(self, excpt: Exception):
        """
        Propagate errors/state to all registered comms
        Parent process, writes to the remote Pipe connection
        """
        for com in self._comms:
            self.comm(com, "remote").send(excpt)

    def notify_parent(self, excpt: Exception):
        """
        Child process, writes to the local Pipe connection
        """
        # Hmmm, child process, are there multiple _comms here?
        for com in self._comms:
            self.comm(com, "local").send(excpt)

    def default_error_handler(self, excpt: Exception):
        """
        Sane default in case none is defined elsewhere
        """
        raise excpt

    def register_error_handler(self, func: Callable):
        """
        Register callable to be notified of remote error conditions
        """
        self.err_hndlr.append(func)

    def checkerror(self):
        """ Parent process, reads the remote Pipe connection
        """
        rdata = False

        for com in self._comms:
            if self.comm(com, "remote").poll(timeout=2):
                rdata = self.comm(com, "remote").receive()

            if isinstance(rdata, Exception):
                if len(self.err_hndlr) > 0:
                    for hndlr in self.err_hndlr:
                        hndlr(rdata)
                else:
                    self.default_error_handler(rdata)

    def register_proc(self, tag: str, calltarget: Callable, arguments: List):
        """
        Add a multiprocess Process onto our stack for managing.
        """
        self._procs[tag] = mp.Process(target=calltarget, args=arguments)

    def register_comm(self, tag: str, args: tuple):
        """
        Add a multiprocess Pipe pair onto our stack for managing.
        """
        self._comms[tag] = {"local": args[0], "remote": args[1]}

    def register_state(self, tag: str, handle: Value):
        """
        Add a multiprocess Value onto our stack for managing.
        """
        self._state[tag] = handle

    @abstractmethod
    def run(self):
        pass


"""
The existence of the functions below is a peculiarity of multiprocessing, pickle and python classes.
Classes can't be simply pickled, which is required by multiprocessing for Process()es. Opportunities for
future improvement here.
"""

def runlock(state: mp.Value, me: str, peers: List, remotecomm: Connection):
    """ So many questions/problems with implementing this remotecomm parameter!
        This is NOT the best way to do this, while keeping DistributedLock generic.
    """
    # XXX - TODO - Pick up here.
    #    e = remotecomm.recv()
    #    if isinstance(e, Exception):

    dl = DistributedLock(me, peers, lockid="coincidenceLock", leader=state)
    dl.run()


def runlistener(
    env_path: str, use_local_db: bool, firedrill_mode: bool, remotecomm: Connection
) -> None:
    al = AlertListener(
        env_path=env_path,
        use_local=use_local_db,
        firedrill_mode=firedrill_mode,
        remotecomm=remotecomm,
    )
    al.run()


def runcoincidence(leader: Value, remotecomm: Connection) -> None:
    server_tag = gethostname() + "_dev"
    coinc = CoincidenceDistributor(
        leader,
        use_local_db=False,
        drop_db=True,
        firedrill_mode=False,
        server_tag=server_tag,
        send_email=False,
        send_slack=False,
        remotecomm=remotecomm,
    )

    return coinc.run_coincidence()


class Director(DistributedBase):
    """
    Coordinate error handling and propagation as well as state between the
    multiple processes in the SNEWS_CS server.
    """

    def __init__(self, dl_enable: bool, dl_endpoint: str, dl_peers: List):
        """

        """
        super().__init__()

        self.distributedlock = dl_enable
        self.dl_endpoint = dl_endpoint
        self.dl_peerenv = dl_peers

        set_env()

        self.register_comm("coinc", mp.Pipe(duplex=True))
        self.register_comm("lock", mp.Pipe(duplex=True))
        self.register_comm("listen", mp.Pipe(duplex=True))

        self.register_state("leader", mp.Value("i", 0, lock=True))

        self.register_proc(
            "coincidence",
            runcoincidence,
            (self.state("leader"), self.comm("coinc", "remote")),
        )
        if self.distributedlock:
            self.register_proc(
                "distributedlock",
                runlock,
                (
                    self.state("leader"),
                    self.dl_me,
                    self.dl_peers,
                    self.comm("lock", "remote"),
                ),
            )
        else:
            self.set_state("leader", True)

        self.register_proc(
            "listener", runlistener, (None, False, False, self.comm("listen", "remote"))
        )

    def run(self):
        print(f"hop version: {hop.__version__}")

        try:
            if self.distributedlock:
                print("Starting distributedlock...")
                self.proc("distributedlock").start()

            print("Starting AlertListener...")
            self.proc("listener").start()

            print("Starting CoincidenceDistributor...")
            self.proc("coincidence").start()

            while True:
                self.checkerror()
                sleep(2)

        except KeyboardInterrupt as e:
            print("Caught a keyboard interrupt. Exiting.")
            log.error("(2) Caught a keyboard interrupt. Exiting.\n")

            """ 
            Propagate this error to the other processes!
            """
            self.broadcast(e)

            self.proc("coincidence").join()
            print("CoincidenceDistributor ended.")
            if self.distributedlock:
                self.proc("distributedlock").join()
                print("distributedlock ended.")

            self.proc("listener").join()
            print("AlertListener ended.")
            sys.exit(1)

        except Exception as e:
            self.broadcast(e)
            raise

        finally:
            self.proc("coincidence").join()
            print("CoincidenceDistributor ended.")
            if self.distributedlock:
                self.proc("distributedlock").join()
                print("distributedlock ended.")

            self.proc("listener").join()
            print("AlertListener ended.")
