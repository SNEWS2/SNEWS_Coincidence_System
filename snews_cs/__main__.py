#!/usr/bin/env python
"""
 The command line tool for
 - Running coincidence logic in the background
 - Simulate data
 - initiate slack bot

"""

import os
from pathlib import Path
from socket import gethostname

# https://click.palletsprojects.com/en/8.0.x/utils/
import click

from . import __version__, cs_utils
from . import snews_coinc as snews_coinc
from .database import Database
from .heartbeat_feedbacks import FeedBack

# Database initialized before any command is called
db = Database(db_file_path=Path(__file__).parent.parent / "snews_cs.db")
db.initialize_database(sql_schema_path=Path(__file__).parent / "db_schema.sql")


@click.group(invoke_without_command=True)
@click.version_option(__version__)
@click.option('--env', type=str,
    default='/etc/test-config.env',
    show_default='etc/test-config.env',
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
def run_coincidence(firedrill, dropdb, email, slackbot):
    """ Initiate Coincidence Decider
    """

    HOST = gethostname()
    coinc = snews_coinc.CoincidenceDistributor(drop_db=dropdb,
                                               firedrill_mode=firedrill,
                                               server_tag=HOST,
                                               send_email=email,
                                               send_slack=slackbot)
    try:
        coinc.run_coincidence()
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
