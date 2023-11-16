import sqlite3
import os
import pandas as pd
import numpy as np
from . import cs_utils
from datetime import datetime, timedelta


class Storage:
    """
    Class for interacting with the SNEWS SQL database.

    Parameters
    ----------
    env : `str`, optional
        Path to env file, defaults to './auxiliary/test-config.env'
    drop_db : `bool`, optional
        drops all items in the DB every time Storage is initialized, defaults to False
    use_local_db : `bool`, optional
        Tells Storage to set up a local DB, defaults to False.

    """

    def __init__(self, env=None, drop_db=True, use_local_db=False):
        cs_utils.set_env(env)
        self.mgs_expiration = int(os.getenv('MSG_EXPIRATION'))
        self.coinc_threshold = int(os.getenv('COINCIDENCE_THRESHOLD'))
        # define the db path
        self.db_path = 'snews_db.db'
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.create_message_tables()
        self.create_alert_tables()
        if drop_db:
            self.drop_tables()
            self.create_message_tables()
            self.create_alert_tables()
    def reconnect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
    def create_message_tables(self):
        """
        Creates the tables in the SQL database.
        tables include: all_mgs, sig_tier_archive, time_tier_archive, coincidence_tier_archive,
        Schema for all_mgs:
        - id: unique id for each message
        - received_time: time message was received
        - message_type: message type
        - message: message content this is a stringified json
        - expiration: expiration time (received time + expiration threshold) of message as datetime in ISO format string

        Schema for sig_tier_archive:
        - _id: unique id for each message string
        - schema_version: version of the schema float
        - detector_name: name of the detector string
        - p_values : p-values for each alert type should be an array of floats
        - neutrino_time: time of initial neutrino in datetime in ISO format string
        - t_bin_width: time bin width in seconds float
        - sent_time: time message was sent datetime in ISO format string
        - machine_time: time message was sent in machine time as datetime in ISO format string
        - meta: meta data for the message this is a stringified json
        - expiration: expiration time (received time + expiration threshold) of message as datetime in ISO format string


        Schema for time_tier_archive:
        - _id: unique id for each message string
        - schema_version: version of the schema float
        - detector_name: name of the detector string
        - p_value: p-value for the alert type float
        - t_bin_width: time bin width in seconds float
        - timing_series: neutrino times seen by detector in datetime in ISO format as array of strings
        - sent_time: time message was sent datatime in ISO format string
        - machine_time: time message was sent in machine time as datetime in ISO format string
        - meta: meta data for the message this is a stringified json
        - expiration: expiration time (received time + expiration threshold) of message as datetime in ISO format string


        Schema for coincidence_tier_archive:
        - _id: unique id for each message string
        - schema_version: version of the schema float
        - detector_name: name of the detector string
        - p_value: p-value for the alert type float
        - neutrino_time: time of initial neutrino in datetime in ISO format string
        - sent_time: time message was sent datatime in ISO format string
        - machine_time: time message was sent in machine time as datetime in ISO format string
        - meta: meta data for the message this is a stringified json
        - expiration: expiration time (received time + expiration threshold) of message as datetime in ISO format string


        """
        self.reconnect()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS all_mgs   
                            (id TEXT, received_time TEXT, message_type TEXT, message TEXT, expiration TEXT)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS sig_tier_archive
                            (_id TEXT, schema_version REAL, detector_name TEXT, p_values TEXT, t_bin_width REAL, is_test INTEGER, sent_time TEXT, machine_time TEXT, meta TEXT, expiration TEXT)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS time_tier_archive
                            (_id TEXT, schema_version REAL, detector_name TEXT, p_value REAL, t_bin_width REAL, timing_series TEXT,  sent_time TEXT, machine_time TEXT, meta TEXT, expiration TEXT)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS coincidence_tier_archive
                            (_id TEXT, schema_version REAL, detector_name TEXT, p_value REAL, neutrino_time TEXT, sent_time TEXT, machine_time TEXT, meta TEXT, expiration TEXT)''')
        self.conn.commit()
        self.conn.close()

    def create_alert_tables(self):
        """
        Creates the tables in the SQL database for alerts.

        Schema for coincidence_tier_alerts:
        - "_id": unique id for each message string
        - "alert_type": alert type string
        - "server_tag": server tag string
        - "False Alarm Prob": false alarm probability float
        - "detector_names": names of detectors in alert as array of strings
        - "sent_time": time message was sent datetime in ISO format string
        - "p_values": p-values for each alert type should be an array of floats
        - "neutrino_times": neutrino times for each alert type should be an array of strings
        - "p_values average": average p-value for alert type float
        - "sub list number": sub list number integer

        Schema for sig_tier_alerts:
        TBD

        Schema for time_tier_alerts:
        TBD
        """
        self.reconnect()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS coincidence_tier_alerts
                            (_id TEXT, alert_type TEXT, server_tag TEXT, "False Alarm Prob" TEXT, detector_names TEXT, sent_time TEXT, p_values TEXT, neutrino_times TEXT, "p_values average" TEXT, "sub list number" INTEGER)''')
        self.conn.commit()
        self.conn.close()
    def drop_tables(self):
        """
        Drops all tables in the SQL database.
        """
        self.reconnect()
        self.cursor.execute('''DROP TABLE IF EXISTS all_mgs''')
        self.cursor.execute('''DROP TABLE IF EXISTS sig_tier_archive''')
        self.cursor.execute('''DROP TABLE IF EXISTS time_tier_archive''')
        self.cursor.execute('''DROP TABLE IF EXISTS coincidence_tier_archive''')
        self.cursor.execute('''DROP TABLE IF EXISTS coincidence_tier_alerts''')
        # self.cursor.execute('''DROP TABLE IF EXISTS sig_tier_alerts''')
        # self.cursor.execute('''DROP TABLE IF EXISTS time_tier_alerts''')
        self.conn.commit()
        self.conn.close()

    def insert_mgs(self, mgs, tier):
        """
        Inserts a message into the all_mgs table.

        Parameters
        ----------
        mgs : `dict`
            dictionary of the SNEWS message

        """
        # to sent time datetime string and expiration datetime string
        exp_date = datetime.fromisoformat(mgs['received_time']) + timedelta(hours=48)
        exp_date = exp_date.isoformat()
        self.reconnect()
        if tier == 'SIG':
            self.cursor.execute('''INSERT INTO all_mgs VALUES (?, ?, ?, ?, ?)''',
                                (mgs['_id'], mgs['received_time'], 'SIG', str(mgs), exp_date))
            self.cursor.execute('''INSERT INTO sig_tier_archive VALUES (?, ?, ?,  ?, ?, ?, ?, ?, ?)''',
                                (mgs['_id'], mgs['schema_version'], mgs['detector_name'], str(mgs['p_values']),
                                 mgs['t_bin_width'], mgs['sent_time'], mgs['machine_time'],
                                 str(mgs['meta']), exp_date))
            self.conn.commit()
            self.conn.close()
        elif tier == 'TIME':
            self.cursor.execute('''INSERT INTO all_mgs VALUES (?, ?, ?, ?, ?)''',
                                (mgs['_id'], mgs['received_time'], 'TIME', str(mgs), exp_date))
            self.cursor.execute('''INSERT INTO time_tier_archive VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                (mgs['_id'], mgs['schema_version'], mgs['detector_name'], mgs['p_value'],
                                 mgs['t_bin_width'], str(mgs['timing_series']), mgs['sent_time'],
                                 mgs['machine_time'], str(mgs['meta']), exp_date))
            self.conn.commit()
            self.conn.close()
        elif tier == 'COINC':
            self.cursor.execute('''INSERT INTO all_mgs VALUES (?, ?, ?, ?, ?)''',
                                (mgs['_id'],mgs['received_time'], 'COINC', str(mgs), exp_date))
            self.cursor.execute('''INSERT INTO coincidence_tier_archive VALUES (?,  ?, ?, ?, ?, ?, ?, ?, ?)''',
                                (mgs['_id'], mgs['schema_version'], mgs['detector_name'], mgs['p_value'],
                                 mgs['neutrino_time'], mgs['sent_time'], mgs['machine_time'],
                                 str(mgs['meta']), exp_date))
            self.conn.commit()
            self.conn.close()

    def insert_alert(self, alert, tier):
        self.reconnect()
        if tier == 'COINC':
            self.cursor.execute('''INSERT INTO coincidence_tier_alerts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                (alert['_id'], alert['alert_type'], alert['server_tag'], alert['False Alarm Prob'],
                                 str(alert['detector_names']), alert['sent_time'], str(alert['p_values']),
                                 str(alert['neutrino_times']), alert['p_values average'], alert['sub list number']))
            self.conn.commit()
            self.conn.close()
        elif tier == 'SIG':
            pass
        elif tier == 'TIME':
            pass

    def drop_expired(self):
        """
        Drops all expired messages from the all_mgs table.
        """
        self.reconnect()
        self.cursor.execute('''DELETE FROM all_mgs WHERE expiration < ?''', (datetime.now().isoformat(),))
        self.cursor.execute('''DELETE FROM sig_tier_archive WHERE expiration < ?''', (datetime.now().isoformat(),))
        self.cursor.execute('''DELETE FROM time_tier_archive WHERE expiration < ?''', (datetime.now().isoformat(),))
        self.cursor.execute('''DELETE FROM coincidence_tier_archive WHERE expiration < ?''',
                            (datetime.now().isoformat(),))
        self.conn.commit()
        self.conn.close()

    def get_all_messages(self, sort_order='ASC'):
        """
        Returns all messages in the all_mgs table.
        """
        self.cursor.execute('''SELECT * FROM all_mgs ORDER BY received_time {}'''.format(sort_order))
        return self.cursor.fetchall()

    def get_all_coinc_alerts(self, sort_order='ASC'):
        """
        Returns all messages in the all_mgs table.
        """
        self.cursor.execute('''SELECT * FROM coincidence_tier_alerts ORDER BY sent_time {}'''.format(sort_order))
        return self.cursor.fetchall()

    def get_all_sig_alerts(self, sort_order='ASC'):
        pass

    def get_all_time_alerts(self, sort_order='ASC'):
        pass

    def get_all_sig_messages(self, sort_order='ASC'):
        """
        Returns all messages in the all_mgs table.
        """
        self.reconnect()
        self.cursor.execute('''SELECT * FROM sig_tier_archive ORDER BY sent_time {}'''.format(sort_order))
        table = self.cursor.fetchall()
        self.conn.close()
        return table

    def get_all_time_messages(self, sort_order='ASC'):
        """
        Returns all messages in the all_mgs table.
        """
        self.reconnect()
        self.cursor.execute('''SELECT * FROM time_tier_archive ORDER BY sent_time {}'''.format(sort_order))
        table = self.cursor.fetchall()
        self.conn.close()
        return table

    def get_all_coinc_messages(self, sort_order='ASC'):
        """
        Returns all messages in the all_mgs table.
        """
        self.reconnect()
        self.cursor.execute('''SELECT * FROM coincidence_tier_archive ORDER BY sent_time {}'''.format(sort_order))
        table =  self.cursor.fetchall()
        self.conn.close()
        return table

    def retract_message(self, message_id, tier):
        """
        Retracts a message from the all_mgs table.

        Parameters
        ----------
        message_id : `str`
            unique id for each message

        """
        self.reconnect()
        self.cursor.execute('''DELETE FROM all_mgs WHERE _id = ?''', (message_id,))
        if tier == 'SIG':
            self.cursor.execute('''DELETE FROM sig_tier_archive WHERE _id = ?''', (message_id,))
        elif tier == 'TIME':
            self.cursor.execute('''DELETE FROM time_tier_archive WHERE _id = ?''', (message_id,))
        elif tier == 'COINC':
            self.cursor.execute('''DELETE FROM coincidence_tier_archive WHERE _id = ?''', (message_id,))
        self.conn.commit()
        self.conn.close()

    def update_message(self,message,tier):
        """
        Updates a message in the all_mgs table and corresponding tier tabe.
        """
        self.reconnect()
        self.cursor.execute('''UPDATE all_mgs SET message = ? WHERE _id = ?''', (str(message),message['_id']))
        if tier == 'SIG':
            # update all columns except _id
            self.cursor.execute('''UPDATE sig_tier_archive SET schema_version = ?, detector_name = ?, p_values = ?, t_bin_width = ?, sent_time = ?, machine_time = ?, meta = ? WHERE _id = ?''', (message['schema_version'], message['detector_name'], str(message['p_values']), message['t_bin_width'], message['sent_time'], message['machine_time'], str(message['meta']), message['_id']))
        elif tier == 'TIME':
            # update all columns except _id
            self.cursor.execute('''UPDATE time_tier_archive SET schema_version = ?, detector_name = ?, p_value = ?, t_bin_width = ?, timing_series = ?, sent_time = ?, machine_time = ?, meta = ? WHERE _id = ?''', (message['schema_version'], message['detector_name'], message['p_value'], message['t_bin_width'], str(message['timing_series']), message['sent_time'], message['machine_time'], str(message['meta']), message['_id']))
        elif tier == 'COINC':
            # update all columns except _id
            self.cursor.execute('''UPDATE coincidence_tier_archive SET schema_version = ?, detector_name = ?, p_value = ?, neutrino_time = ?, sent_time = ?, machine_time = ?, meta = ? WHERE _id = ?''', (message['schema_version'], message['detector_name'], message['p_value'], message['neutrino_time'], message['sent_time'], message['machine_time'], str(message['meta']), message['_id']))
        self.conn.commit()
        self.conn.close()

    def show_tables(self):
        """
        Returns all tables in the SQL database.
        """
        self.reconnect()
        self.cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' ORDER BY name''')
        table = self.cursor.fetchall()
        self.conn.close()
        return table

    def get_table_schema(self, table_name):
        """
        Returns the schema for a given table.
        """
        self.reconnect()
        self.cursor.execute('''PRAGMA table_info({})'''.format(table_name))
        schema = self.cursor.fetchall()
        self.conn.close()
        return schema

    def insert_coinc_cache(self, cache):
        """
        Inserts coincidence cache dataframe into the coincidence_tier_archive table.

        Parameters
        ----------
        cache : dataframe
            dictionary of the SNEWS message

        """
        # to sent time datetime string and expiration datetime string
        exp_date = datetime.fromisoformat(cache['sent_time'][0]) + timedelta(hours=48)
        exp_date = exp_date.isoformat()
        self.reconnect()
        # coincidence_tier_archive is not empty delete all rows
        self.cursor.execute('''DELETE FROM coincidence_tier_archive''')

        # insert dataframe into table
        for index, row in cache.iterrows():
            self.cursor.execute('''INSERT INTO coincidence_tier_archive VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                (row['_id'], row['schema_version'], row['detector_name'], row['p_value'],
                                 row['neutrino_time'], row['sent_time'], row['machine_time'],
                                 str(row['meta']), exp_date))
        self.conn.commit()
        self.conn.close()







