#!/usr/bin/env python

## TO-DO
## Allow Heartbeat command to accept external detector status info
## Allow Publish observation to accept a json file
## Prevent unauthorized to publish alert topics? (likely not needed)

# https://click.palletsprojects.com/en/8.0.x/utils/
import click
from . import __version__
from . import hop_pub
from . import hop_sub
from . import snews_utils
from . import snews_coinc
from . import snews_retract

# Kara: Testing PyCharm - Git interface
def get_commit_message(bypass, topic):
    """ Parse the commited message, take input if not bypassed
        Set the message structure based on input and topic.

    """
    header = "\n# Modify the message as desired\n\n\n#"+30*"-"+"\n"
    curr_time = snews_utils.TimeStuff().get_snews_time("%H:%M:%S:%f")

    if topic == 'A':
        editables = snews_utils.data_alert()
        editables['machine_times'] = (curr_time, curr_time)
        editables['neutrino_times'] = (curr_time, curr_time)
    else:
        editables = snews_utils.data_obs()
        editables['machine_time'] = curr_time
        editables['neutrino_time'] = curr_time
    
    # ask for user input if not bypassed
    if not bypass:
        pretty_editables = '\n'.join([f'{k:<20s}:{v}' for k,v in editables.items()])+'\n'
        message = click.edit(header + pretty_editables + '\n\n', editor='nano')
        if message is not None:
            input_ = message.split(header, 1)[1].rstrip('\n')
            lines = input_.split('\n')
            message = {line.split(':', maxsplit=1)[0].strip() : line.split(':', maxsplit=1)[1] for line in lines}
            message = {k:v.strip() for k,v in message.items() if type(v)==str} # cosmetics
    else : message = editables
    return message

def set_topic(topic, env):
    if len(topic)>1 : topic=topic[0]
    topic_tuple = snews_utils.set_topic_state(topic, env)
    topic_broker = topic_tuple.topic_broker
    return topic_broker


@click.group(invoke_without_command=True)
@click.version_option(__version__)
@click.option('--env', type=str, 
    default='./hop_comms/auxiliary/test-config.env',
    show_default='auxiliary/test-config.env', 
    help='environment file containing the configurations')
def main(env):
    """ User interface for hop_comm tools
    """
    snews_utils.set_env(env)


@main.command()
@click.option('--topic','-t', type=str, default='O', show_default='snews-observations', help='Selected kafka topic [O/A]')
@click.option('--broker','-b', type=str, default='None', show_default='from env variables', help='Selected kafka topic')
@click.option('--experiment','-e', type=str, default="TEST", show_default='test experiment properties')
@click.option('--tier','-ti', type=str, default="CoincidenceTier", show_default='Coincidence Tier')
@click.option('--bypass/--no-bypass', default=True, show_default='True', help='if False, asks user to modify the content of a message')
@click.option('--env', default=None, show_default='test-config.env', help='environment file containing the configurations')
@click.option('--local/--no-local', default=True, show_default='True', help='Whether to use local database server or take from the env file')
def publish(topic, broker, experiment, tier, bypass, env, local):
    """ Publish a message using hop_pub

    Notes
    -----
    If neither broker nor env filepath is given, first checks if the topic
    is set in the environment (i.e. os.getenv('X_TOPIC')). If not, sets 
    this topic from the defaults i.e. from auxiliary/test-config.env
    If a different broker than that is set by the environment variables
    is passed. This overwrites the existing broker at the given topic.
    ::: if a different broker is given we can also make it the new env var 
    """
    click.clear()
    if broker == 'None': broker = set_topic(topic, env)
    # get the message
    data = get_commit_message(bypass, topic)
    if topic.upper()=='A':
        pub = hop_pub.Publish_Alert(use_local=local)
        topic_col = 'red'
        append = ''
        contents = (tier+'Alert', data)
    else:
        pub = hop_pub.Publish_Tier_Obs()
        topic_col = 'blue'
        append = f'\n\t==> {tier}'.expandtabs(25)
        contents = (experiment, tier, data)

    click.echo('\n> Welcome '+click.style(experiment, fg='blue', bg='white', bold=True, blink=True)+' !')
    click.secho(f'>> You are publishing to {broker} {append}', fg='white', bg=topic_col, bold=False)
    # publish 
    pub.publish(*contents)


@main.command()
@click.option('--broker','-b', type=str, default='None', show_default='from env variables', help='Selected kafka topic')
@click.option('--env', default=None, show_default='test-config.env', help='environment file containing the configurations')
@click.option('--experiment','-e', type=str, default="TEST", show_default='test experiment properties')
@click.option('--rate','-r', default=60, nargs=1, help='rate in sec to send heartbeat messages')
def publish_heartbeat(broker, env, experiment, rate):
    if broker == 'None': broker = set_topic('H', env)
    click.secho(f'>> You are publishing Heartbeat messages to {broker}\n>> with {rate} seconds intervals', fg='white', bg='blue', bold=False)
    pub = hop_pub.Publish_Heartbeat(rate=rate, detector=experiment)
    pub.publish()

@main.command()
@click.option('--local/--no-local', default=True, show_default='True', help='Whether to use local database server or take from the env file')
def retract(local):
    retraction = snews_retract.Retraction(local)
    retraction.run_retraction()
    ### This should not work like this. Instead, it should submit a message using publish.
    ### with FalseOBS as its tier


@main.command()
@click.option('--local/--no-local', default=True, show_default='True', help='Whether to use local database server or take from the env file')
@click.option('--hype/--no-hype', default=False, show_default='False', help='Whether to run in hype mode')
def run_coincidence(local, hype):
    """ 
    """
    click.echo('Initial implementation. Likely to change')
    # # Initiate Coincidence Decider
    coinc = snews_coinc.CoincDecider(use_local=local, hype_mode_ON=hype)
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
def simulate(rate, alert_probability):
    """ Simulate Observation Messages
    """
    import numpy as np
    import time
    click.secho(f'Simulating observation messages every {rate} sec\n\
        with a {alert_probability*100}% alert probability ', fg='blue', bg='white', bold=True)
    detectors = snews_utils.retrieve_detectors()
    try:
        while True:
            detector = np.random.choice(list(detectors.keys()))
            if np.random.random() < alert_probability:
                data = get_commit_message(True, topic='O')
                pub = hop_pub.Publish_Tier_Obs()
                pub.publish(detector, 'CoincidenceTier', data)
            time.sleep(rate)
    except KeyboardInterrupt:
        pass
    finally:
        click.secho(f'\n{"="*30}DONE{"="*30}', fg='white', bg='green')

if __name__ == "__main__":
    main()