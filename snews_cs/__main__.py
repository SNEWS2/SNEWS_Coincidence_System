#!/usr/bin/env python
"""
 The command line tool for
 - Running coincidence logic in the background
 - Simulate data
 - iniate slack bot

 TODO:
 -> Storage will change, and tools will need to be updated
 -> Allow Heartbeat command to accept external detector status info
 ->
"""

# https://click.palletsprojects.com/en/8.0.x/utils/
import click
from . import __version__
import sys, os
sys.path.append('../../SNEWS_Publishing_Tools/')
from SNEWS_PT import snews_pt_utils
from SNEWS_PT.snews_pub import Publisher, CoincidenceTier
from . import snews_utils
from . import snews_coinc
from .simulate import randomly_select_detector


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
    snews_pt_utils.set_env(env_path)
    ctx.obj['env'] = env

@main.command()
@click.option('--local/--no-local', default=True, show_default='True', help='Whether to use local database server or take from the env file')
@click.option('--hype/--no-hype', default=False, show_default='True', help='Whether to run in hype mode')
def run_coincidence(local, hype):
    """ 
    """
    click.echo('Initial implementation. Likely to change')
    # # Initiate Coincidence Decider
    coinc = snews_coinc.CoincDecider(use_local_db=local, hype_mode_ON=hype)
    try: coinc.run_coincidence()
    except KeyboardInterrupt: pass
    finally: click.secho(f'\n{"="*30}DONE{"="*30}', fg='white', bg='green')


@main.command()
@click.option('--test/--no-test', default=True, show_default='True', help='should the bot tag the channel')
def run_slack_bot(test):
    """
    """
    test = 1 if test else 0
    os.system(f'python3 snews_bot.py {test}')


@main.command()
@click.option('--rate','-r', default=1, nargs=1, help='rate to send observation messages in sec')
@click.option('--alert_probability','-p', default=0.2, nargs=1, help='probability for an alert')
@click.pass_context
def simulate(ctx, rate, alert_probability):
    """ Simulate Observation Messages
    """
    import numpy as np
    import time
    times = snews_pt_utils.TimeStuff()
    click.secho(f'Simulating observation messages every {rate} sec\n\
        with a {alert_probability*100}% alert probability ', fg='blue', bg='white', bold=True)
    try:
        while True:
            detector = randomly_select_detector()
            if np.random.random() < alert_probability:
                data = snews_pt_utils.coincidence_tier_data()
                data['detector_name'] = detector
                data['neutrino_time'] = times.get_snews_time('%H:%M:%S:%f')
                message = CoincidenceTier(**data).message()
                pub = ctx.with_resource(Publisher(verbose=True))
                pub.send(message)
            time.sleep(rate)
    except KeyboardInterrupt:
        pass
    finally:
        click.secho(f'\n{"="*30}DONE{"="*30}', fg='white', bg='green')

if __name__ == "__main__":
    main()