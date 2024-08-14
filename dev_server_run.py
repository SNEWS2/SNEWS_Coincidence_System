"""
Start the server in 'development' mode.
    -test topics, no slack, no email, _dev suffix on server tag.
"""

from typing import List
import os

from snews_cs.cs_director import Director


def main():
    peers = ()

    me = os.getenv('DISTRIBUTED_LOCK_ENDPOINT')
    peerenv = os.getenv('DISTRIBUTED_LOCK_PEERS')
    if peerenv is not None:
        peers = peerenv.split(',')

    director = Director(True, me, peers)
    director.run()


if __name__ == '__main__':
    main()