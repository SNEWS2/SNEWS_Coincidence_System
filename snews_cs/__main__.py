#!/usr/bin/env python
"""
 The command line tool for
 - Running coincidence logic in the background
 - Simulate data
 - initiate slack bot

"""

# https://click.palletsprojects.com/en/8.0.x/utils/
import click, os
from . import __version__
from . import snews_utils
from . import snews_coinc
from .simulate import randomly_select_detector, get_simulated_message


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
    snews_utils.set_env(env_path)
    ctx.obj['env'] = env

@main.command()
@click.option('--local/--no-local', default=True, show_default='True', help='Whether to use local database server or take from the env file')
@click.option('--hype/--no-hype', default=True, show_default='True', help='Whether to run in hype mode')
def run_coincidence(local, hype):
    """ 
    """
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

def display_message(message):
    click.secho(f'{"-" * 57}', fg='bright_blue')
    if message['_id'].split('_')[1] == 'FalseOBS':
        click.secho("It's okay, we all make mistakes".upper(), fg='magenta')
    for k, v in message.items():
        print(f'{k:<20s}:{v}')

if __name__ == "__main__":
    main()