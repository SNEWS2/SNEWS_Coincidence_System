#!/usr/bin/env python
"""
 The command line tool for
 - Running coincidence logic in the background
 - Simulate data
 - initiate slack bot

"""

from socket import gethostname
import multiprocessing as mp
import os

# https://click.palletsprojects.com/en/8.0.x/utils/
import click
from rich.console import Console

from distributed.lock import DistributedLock

from . import __version__
from . import cs_utils
from . import snews_coinc as snews_coinc
from . heartbeat_feedbacks import FeedBack

def runlock(state: mp.Value, me: str, peers: List):
    dl = DistributedLock(me, peers)
    dl.run(state)

@click.group(invoke_without_command=True)
@click.version_option(__version__)
@click.option('--env', type=str,
    default='/auxiliary/test-config.env',
    show_default='auxiliary/test-config.env',
    help='environment file containing the configurations')
@click.pass_context
def main(ctx, env):
    """ User interface for snews_pt tools
    """
    base = os.path.dirname(os.path.realpath(__file__))
    env_path = base + env
    ctx.ensure_object(dict)
    cs_utils.set_env(env_path)
    ctx.obj['env'] = env

@main.command()
@click.option('--firedrill/--no-firedrill', default=False, show_default='False', help='Whether to use firedrill brokers or default ones')
@click.option('--dropdb/--no-dropdb', default=True, show_default='True', help='Whether to drop the current database')
@click.option('--email/--no-email', default=True, show_default='True', help='Whether to send emails along with the alert')
@click.option('--slackbot/--no-slackbot', default=True, show_default='True', help='Whether to send the alert on slack')

@click.option('--distributedlock/--no-distributedlock', default=False, show_default='True', help='Run distributed locking subsystem.')
def run_coincidence(local, firedrill, dropdb, email, slackbot):
    """ Initiate Coincidence Decider 
    """
    # Globally
    #mp.set_start_method('spawn')
    leader = mp.Value('i', 0, lock=True)

    me = os.getenv('DISTRIBUTED_LOCK_ENDPOINT')
    peerenv = os.getenv('DISTRIBUTED_LOCK_PEERS')
    if peerenv is not None:
        peers = peerenv.split(',')


    server_tag = gethostname()
    coinc = snews_coinc.CoincidenceDistributor(drop_db=dropdb,
                                               firedrill_mode=firedrill,
                                               server_tag=server_tag,
                                               send_email=email,
                                               send_slack=slackbot)

    try:
        coincidenceproc = mp.Process(target=coinc.run_coincidence, args=(leader,))
        coincidenceproc.start()

        if distributedlock:
            distributedlockproc = mp.Process(target=runlock, args=(leader, me, peers))
            distributedlockproc.start()
        else:
            leader = True

        listenproc = mp.Process(target=coinc.run_alert_listener)
        listenproc.start()

        coincidenceproc.join()
        if distributedlock:
            distributedlockproc.join()

        listenproc.join()

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(e)
    finally:
        click.secho(f'\n{"="*30}DONE{"="*30}', fg='white', bg='green')


@main.command()
@click.option('--verbose', '-v', default=False, show_default='False', help='Verbose print')
def run_feedback(verbose):
    """ Start the feedback checks
    """
    feedback = FeedBack(verbose=verbose)
    click.secho(f'\nInvoking Feedback search, verbose={verbose}\n', fg='white', bg='green')
    feedback()


if __name__ == "__main__":
    main()
