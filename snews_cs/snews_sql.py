import inspect
import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from . import cs_utils
from .database import Database

# Get the directory of the script
current_script_directory = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe()))
)

# Go back one level to the parent directory
parent_directory = os.path.join(current_script_directory, os.pardir)


class Storage:
    """
    Class for interacting with the SNEWS SQL database.

    Parameters
    ----------
    env : `str`, optional
        Path to env file, defaults to './etc/test-config.env'
    drop_db : `bool`, optional
        drops all items in the DB every time Storage is initialized, defaults to False

    """

    def __init__(self, env=None, drop_db=True):
        cs_utils.set_env(env)
        self.mgs_expiration = int(os.getenv("MSG_EXPIRATION"))
        self.coinc_threshold = int(os.getenv("COINCIDENCE_THRESHOLD"))
        self.db_path = os.path.join(parent_directory, "snews_cs.db")
        self.db = Database(db_file_path=self.db_path)
        self.conn = self.db.connection
        self.cursor = self.db.cursor

        if drop_db:
            self.db.drop_tables(
                table_names=[
                    "all_mgs",
                    "sig_tier_archive",
                    "time_tier_archive",
                    "coincidence_tier_archive",
                    "coincidence_tier_alerts",
                ]
            )
            self.db.initialize_database(sql_schema_path=Path(__file__).parent / "db_schema.sql")

    def insert_mgs(self, mgs, tier):
        """
        Inserts a message into the all_mgs table.

        Parameters
        ----------
        mgs : `dict`
            dictionary of the SNEWS message

        """
        # to sent time datetime string and expiration datetime string
        expiration = datetime.fromisoformat(mgs["received_time"]) + timedelta(hours=48)
        expiration = expiration.isoformat()

        # MK: proposed change
        # expiration = np.datetime64(mgs['received_time'][0]) + np.timedelta64(48, 'h')
        # expiration = np.datetime_as_string(expiration, unit='ns')

        if tier == "SIG":
            self.cursor.execute(
                """INSERT INTO all_mgs VALUES (?, ?, ?, ?, ?)""",
                (mgs["id"], mgs["received_time"], "SIG", str(mgs), expiration),
            )
            self.cursor.execute(
                """INSERT INTO sig_tier_archive VALUES (?, ?, ?,  ?, ?, ?, ?, ?, ?)""",
                (
                    mgs["id"],
                    mgs["schema_version"],
                    mgs["detector_name"],
                    str(mgs["p_vals"]),
                    mgs["t_bin_width_sec"],
                    mgs["sent_time_utc"],
                    mgs["machine_time_utc"],
                    str(mgs["meta"]),
                    expiration,
                ),
            )
            self.conn.commit()

        elif tier == "TIME":
            self.cursor.execute(
                """INSERT INTO all_mgs VALUES (?, ?, ?, ?, ?)""",
                (mgs["id"], mgs["received_time"], "TIME", str(mgs), expiration),
            )
            self.cursor.execute(
                """INSERT INTO time_tier_archive VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    mgs["id"],
                    mgs["schema_version"],
                    mgs["detector_name"],
                    mgs["p_val"],
                    mgs["t_bin_width_sec"],
                    str(mgs["timing_series"]),
                    mgs["sent_time_utc"],
                    mgs["machine_time_utc"],
                    str(mgs["meta"]),
                    expiration,
                ),
            )
            self.conn.commit()

        elif tier == "COINC":
            self.cursor.execute(
                """INSERT INTO all_mgs VALUES (?, ?, ?, ?, ?)""",
                (mgs["id"], mgs["received_time"], "COINC", str(mgs), expiration),
            )
            self.cursor.execute(
                """INSERT INTO coincidence_tier_archive VALUES (?,  ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    mgs["id"],
                    mgs["schema_version"],
                    mgs["detector_name"],
                    mgs["p_val"],
                    mgs["neutrino_time_utc"],
                    mgs["sent_time_utc"],
                    mgs["machine_time_utc"],
                    str(mgs["meta"]),
                    expiration,
                ),
            )
            self.conn.commit()

    def insert_alert(self, alert, tier):

        if tier == "COINC":
            self.cursor.execute(
                """INSERT INTO coincidence_tier_alerts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    alert["id"],
                    alert["alert_type"],
                    alert["server_tag"],
                    alert["false_alarm_prob"],
                    str(alert["detector_names"]),
                    alert["sent_time_utc"],
                    str(alert["p_vals"]),
                    str(alert["neutrino_times"]),
                    alert["p_vals_average"],
                    alert["sub_list_number"],
                ),
            )
            self.conn.commit()

        elif tier == "SIG":
            pass
        elif tier == "TIME":
            pass

    def drop_expired(self):
        """
        Drops all expired messages from the all_mgs table.
        """

        self.cursor.execute(
            """DELETE FROM all_mgs WHERE expiration < ?""",
            (datetime.now().isoformat(),),
        )
        self.cursor.execute(
            """DELETE FROM sig_tier_archive WHERE expiration < ?""",
            (datetime.now().isoformat(),),
        )
        self.cursor.execute(
            """DELETE FROM time_tier_archive WHERE expiration < ?""",
            (datetime.now().isoformat(),),
        )
        self.cursor.execute(
            """DELETE FROM coincidence_tier_archive WHERE expiration < ?""",
            (datetime.now().isoformat(),),
        )
        self.conn.commit()

    def get_all_messages(self, sort_order="ASC"):
        """
        Returns all messages in the all_mgs table.
        """
        self.cursor.execute(
            """SELECT * FROM all_mgs ORDER BY received_time {}""".format(sort_order)
        )
        return self.cursor.fetchall()

    def get_all_coinc_alerts(self, sort_order="ASC"):
        """
        Returns all messages in the all_mgs table.
        """
        self.cursor.execute(
            """SELECT * FROM coincidence_tier_alerts ORDER BY sent_time_utc {}""".format(
                sort_order
            )
        )
        return self.cursor.fetchall()

    def get_all_sig_alerts(self, sort_order="ASC"):
        pass

    def get_all_time_alerts(self, sort_order="ASC"):
        pass

    def get_all_sig_messages(self, sort_order="ASC"):
        """
        Returns all messages in the all_mgs table.
        """

        self.cursor.execute(
            """SELECT * FROM sig_tier_archive ORDER BY sent_time_utc {}""".format(
                sort_order
            )
        )
        table = self.cursor.fetchall()
        return table

    def get_all_time_messages(self, sort_order="ASC"):
        """
        Returns all messages in the all_mgs table.
        """

        self.cursor.execute(
            """SELECT * FROM time_tier_archive ORDER BY sent_time_utc {}""".format(
                sort_order
            )
        )
        table = self.cursor.fetchall()
        return table

    def get_all_coinc_messages(self, sort_order="ASC"):
        """
        Returns all messages in the all_mgs table.
        """

        self.cursor.execute(
            """SELECT * FROM coincidence_tier_archive ORDER BY sent_time_utc {}""".format(
                sort_order
            )
        )
        table = self.cursor.fetchall()

        return table

    def retract_message(self, message_id, tier):
        """
        Retracts a message from the all_mgs table.

        Parameters
        ----------
        message_id : `str`
            unique id for each message

        """

        self.cursor.execute(
            """DELETE FROM all_mgs WHERE message_id = ?""", (message_id,)
        )
        if tier == "SIG":
            self.cursor.execute(
                """DELETE FROM sig_tier_archive WHERE message_id = ?""", (message_id,)
            )
        elif tier == "TIME":
            self.cursor.execute(
                """DELETE FROM time_tier_archive WHERE message_id = ?""", (message_id,)
            )
        elif tier == "COINC":
            self.cursor.execute(
                """DELETE FROM coincidence_tier_archive WHERE message_id = ?""",
                (message_id,),
            )
        self.conn.commit()

    def update_message(self, message, tier):
        """
        Updates a message in the all_mgs table and corresponding tier tabe.
        """

        self.cursor.execute(
            """UPDATE all_mgs SET message = ? WHERE message_id = ?""",
            (str(message), message["id"]),
        )
        if tier == "SIG":
            # update all columns except _id
            self.cursor.execute(
                """UPDATE sig_tier_archive
                SET schema_version = ?, detector_name = ?, p_vals = ?, t_bin_width_sec = ?,
                    sent_time_utc = ?, machine_time_utc = ?, meta = ?
                WHERE message_id = ?""",
                (
                    message["schema_version"],
                    message["detector_name"],
                    str(message["p_vals"]),
                    message["t_bin_width_sec"],
                    message["sent_time_utc"],
                    message["machine_time_utc"],
                    str(message["meta"]),
                    message["id"],
                ),
            )
        elif tier == "TIME":
            # update all columns except _id
            self.cursor.execute(
                """UPDATE time_tier_archive
                SET schema_version = ?, detector_name = ?, p_val = ?, t_bin_width_sec = ?,
                    timing_series = ?, sent_time_utc = ?, machine_time_utc = ?, meta = ?
                WHERE message_id = ?""",
                (
                    message["schema_version"],
                    message["detector_name"],
                    message["p_val"],
                    message["t_bin_width_sec"],
                    str(message["timing_series"]),
                    message["sent_time_utc"],
                    message["machine_time_utc"],
                    str(message["meta"]),
                    message["id"],
                ),
            )
        elif tier == "COINC":
            # update all columns except _id
            self.cursor.execute(
                """UPDATE coincidence_tier_archive
                SET schema_version = ?, detector_name = ?, p_val = ?, neutrino_time_utc = ?,
                    sent_time_utc = ?, machine_time_utc = ?, meta = ?
                WHERE message_id = ?""",
                (
                    message["schema_version"],
                    message["detector_name"],
                    message["p_val"],
                    message["neutrino_time_utc"],
                    message["sent_time_utc"],
                    message["machine_time_utc"],
                    str(message["meta"]),
                    message["id"],
                ),
            )
        self.conn.commit()

    def show_tables(self):
        """
        Returns all tables in the SQL database.
        """

        self.cursor.execute(
            """SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"""
        )
        table = self.cursor.fetchall()

        return table

    def get_table_schema(self, table_name):
        """
        Returns the schema for a given table.
        """

        self.cursor.execute("""PRAGMA table_info({})""".format(table_name))
        schema = self.cursor.fetchall()

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
        expiration = np.datetime64(cache["sent_time_utc"][0]) + np.timedelta64(48, "h")
        expiration = np.datetime_as_string(expiration, unit="ns")

        # expiration = datetime.fromisoformat(cache['sent_time'][0]) + timedelta(hours=48)
        # expiration = expiration.isoformat()

        # coincidence_tier_archive is not empty delete all rows
        self.cursor.execute("""DELETE FROM coincidence_tier_archive""")

        # insert dataframe into table
        insert_query = """
                INSERT INTO coincidence_tier_archive (
                    message_id, schema_version, detector_name, p_val,
                    neutrino_time_utc, sent_time_utc, machine_time_utc, meta, expiration
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

        try:
            for index, row in cache.iterrows():
                self.cursor.execute(
                    insert_query,
                    (
                        row["id"],
                        row["schema_version"],
                        row["detector_name"],
                        row["p_val"],
                        row["neutrino_time_utc"],
                        row["sent_time_utc"],
                        row["machine_time_utc"],
                        str(row["meta"]),
                        expiration,
                    ),
                )

            self.conn.commit()
        except Exception as e:
            # Log the error and rollback the transaction if needed
            print(f"Error inserting data: {e}")
            self.conn.rollback()

    def retrieve_coinc_cache(self):
        """
        Returns coincidence cache dataframe from the coincidence_tier_archive table and saves it
        as a dataframe.
        """
        self.cursor.execute("""SELECT * FROM coincidence_tier_archive""")
        table = self.cursor.fetchall()
        return pd.DataFrame(
            table,
            columns=[
                "message_id",
                "schema_version",
                "detector_name",
                "p_val",
                "neutrino_time_utc",
                "sent_time_utc",
                "machine_time_utc",
                "meta",
                "expiration",
            ],
        )
