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
from . import cs_utils
from . import snews_coinc


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
@click.option('--local/--no-local', default=True, show_default='True', help='Whether to use local database server or take from the env file')
@click.option('--firedrill/--no-firedrill', default=True, show_default='True', help='Whether to use firedrill brokers or default ones')
@click.option('--dropdb/--no-dropdb', default=True, show_default='True', help='Whether to drop the current database')
@click.option('--email/--no-email', default=True, show_default='True', help='Whether to send emails along with the alert')
@click.option('--slackbot/--no-slackbot', default=True, show_default='True', help='Whether to send the alert on slack')
def run_coincidence(local, firedrill, dropdb, email, slackbot):
    """ Initiate Coincidence Decider 
    """
    server_tag = os.getenv('hostname')
    coinc = snews_coinc.CoincDecider(use_local_db=local,
                                     drop_db=dropdb,
                                     firedrill_mode=firedrill,
                                     send_email=email,
                                     server_tag=server_tag,
                                     send_on_slack=slackbot)
    try: 
        coinc.run_coincidence()
    except KeyboardInterrupt: 
        pass
    finally: 
        click.secho(f'\n{"="*30}DONE{"="*30}', fg='white', bg='green')

if __name__ == "__main__":
    main()