#!/usr/bin/env python

## TO-DO
## Allow Heartbeat command to accept external detector status info
## Allow Publish observation to accept a json file
## Prevent unauthorized to publish alert topics? (likely not needed)

# https://click.palletsprojects.com/en/8.0.x/utils/
import click
from . import __version__
from . import hop_pub
import sys
sys.path.append('../../SNEWS_Publishing_Tools/')
from SNEWS_PT import snews_pt_utils
from SNEWS_PT.snews_pub import Publisher, CoincidenceTier
from . import snews_utils
from . import snews_coinc_v2 as snews_coinc
from .simulate import randomly_select_detector


def set_topic(topic, env):
    if len(topic)>1 : topic=topic[0]
    topic_tuple = snews_utils.set_topic_state(topic, env)
    topic_broker = topic_tuple.topic_broker
    return topic_broker

@click.group(invoke_without_command=True)
@click.version_option(__version__)
@click.option('--env', type=str,
    default='./SNEWS_PT/auxiliary/test-config.env',
    show_default='auxiliary/test-config.env',
    help='environment file containing the configurations')
def main(env):
    """ User interface for snews_pt tools
    """
    snews_utils.set_env(env)

@main.command()
@click.option('--local/--no-local', default=True, show_default='True', help='Whether to use local database server or take from the env file')
@click.option('--hype/--no-hype', default=False, show_default='False', help='Whether to run in hype mode')
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
@click.argument('topic', default='A', nargs=1)
@click.option('--broker','-b', type=str, default='None', show_default='from env variables', help='Selected kafka topic')
@click.option('--env', default=None, show_default='test-config.env', help='environment file containing the configurations')
@click.option('--verbose/--no-verbose', default=True, help='verbose output')
@click.option('--local/--no-local', default=True, show_default='True', help='Whether to use local database server or take from the env file')
def subscribe(topic, broker, env, verbose, local):
    """ subscribe to a topic
        If a broker
    """
    click.clear()
    sub = hop_sub.HopSubscribe(use_local=local)
    # if no explicit broker is given, set topic with env
    # if env is also None, this uses default env.
    if broker == 'None': _ = set_topic(topic, env)
    # if a broker is also given, overwrite and use the given broker
    if broker != 'None': topic = broker    
    # click.secho(f'You are subscribing to '+click.style(top, fg='white', bg='bright_blue', bold=True))
    try: sub.subscribe(which_topic=topic, verbose=verbose)
    except KeyboardInterrupt:  pass
    finally: click.secho(f'\n{"="*30}DONE{"="*30}', fg='white', bg='green')

@main.command()
@click.option('--rate','-r', default=1, nargs=1, help='rate to send observation messages in sec')
@click.option('--alert_probability','-p', default=0.2, nargs=1, help='probability for an alert')
@click.pass_context
def simulate(ctx, rate, alert_probability):
    """ Simulate Observation Messages
    """
    import numpy as np
    import time
    click.secho(f'Simulating observation messages every {rate} sec\n\
        with a {alert_probability*100}% alert probability ', fg='blue', bg='white', bold=True)
    try:
        while True:
            detector = randomly_select_detector()
            if np.random.random() < alert_probability:
                data = snews_pt_utils.coincidence_tier_data()
                data['detector_name'] = detector
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