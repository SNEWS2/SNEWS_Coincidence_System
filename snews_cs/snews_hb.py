"""
This a module to handle all heartbeat related work

"""

import os, json
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from .cs_utils import set_env, make_beat_directory
from .core.logging import getLogger

log = getLogger(__name__)

# Check if detector name is in registered list.
detector_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'auxiliary/detector_properties.json'))
with open(detector_file) as file:
    snews_detectors = json.load(file)
snews_detectors = list(snews_detectors.keys())

def get_data_strings(df_input):
    """ Convert datetime objects to strings

    """
    df = df_input.copy()
    type_dtime = type(datetime.utcnow())
    for col in df.columns:
        for i in range(len(df)):
            if type(df[col].iloc[i]) == type_dtime:
                # print(col, i, df[col].iloc[i])
                df.at[i, col] = df.at[i, col].isotime()
    return df


## TODO: make a list of internal heartbeats, and send us = SERVER heartbeats.
# How many times a day can server log these heartbeats, (what is the livetime of server)

class HeartBeat:
    """ Class to handle heartbeat message stream
    """

    def __init__(self, env_path=None, store=True, firedrill_mode=True):
        """
        :param store: `bool`
        """
        log.info("\t> Heartbeat Instance is created.")
        set_env(env_path)
        self.store = store
        self.beats_path = os.path.join(os.path.dirname(__file__), "../beats")
        log.info(f"\t> Heartbeats are stored in {self.beats_path}.")
        make_beat_directory(self.beats_path)
        self.stash_time = float(os.getenv("HB_STASH_TIME", "24"))  # hours
        self.delete_after = float(os.getenv("HB_DELETE_AFTER", "7"))  # days
        if firedrill_mode:
            self.heartbeat_topic = os.getenv("FIREDRILL_OBSERVATION_TOPIC")
        else:
            self.heartbeat_topic = os.getenv("OBSERVATION_TOPIC")

        self.column_names = ["Received Times", "Detector", "Stamped Times", "Latency", "Time After Last", "Status"]
        self.cache_df = pd.DataFrame(columns=self.column_names)

    def make_entry(self, message):
        """ Make an entry in the cache df using new message
        """
        msg = {"Received Times": message["Received Times"],
               "Detector": message["detector_name"],
               "Status": message["detector_status"]}

        stamped_time_obj = datetime.fromisoformat(message["sent_time"])
        msg["Stamped Times"] = stamped_time_obj
        msg["Latency"] = msg["Received Times"] - msg["Stamped Times"]

        # check the last message of given detector
        detector_df = self.cache_df[self.cache_df["Detector"] == msg['Detector']]
        if len(detector_df):
            msg["Time After Last"] = (msg["Received Times"] - detector_df["Received Times"].max()).total_seconds()
        else:
            msg["Time After Last"] = 0 #timedelta(0)

        # add this new entry to cache
        self.cache_df = pd.concat([self.cache_df, pd.DataFrame([msg])], ignore_index=True)

    def store_beats(self):
        """ log the heartbeats, and save locally

        """
        # for now store one master csv in any case
        self.master_csv()
        if self.store:
            self.dump_csv()
            self.dump_JSON()

    def drop_old_messages(self):
        """ Keep the heartbeats for 24 hours
            Drop the earlier-than-24hours messages from cache

        """
        # first store a csv/JSON before dumping anything
        self.store_beats()
        curr_time = datetime.utcnow()
        existing_times = self.cache_df["Received Times"]
        del_t = (curr_time - existing_times).dt.total_seconds() / 60 / 60 # in hours
        locs = np.where(del_t < self.stash_time)[0] # keep only times within stash time e.g. 24 hours
        self.cache_df = self.cache_df.reset_index(drop=True).loc[locs]
        self.cache_df.sort_values(by=['Received Times'], inplace=True)

    def update_cache_csv(self):
        """ The daily csv file is kept as a file per day
            For heartbeat checks I need one that mirrors the current cache.
        """
        mirror_csv = os.path.join(self.beats_path, f"cached_heartbeats_mirror.csv")
        if os.path.exists(mirror_csv):
            self.cache_df.to_csv(mirror_csv, mode='a', header=False, index=False)
        else:
            self.cache_df.to_csv(mirror_csv, mode='w', header=True, index=False)

    def dump_csv(self):
        """ dump a local csv file once a day
            and keep appending the messages within that day
            into the same csv file

            Note:
                This is a temporary back up of the data. Ideally, we need only update_cache_csv

        """
        today = datetime.utcnow()
        today_str = datetime.strftime(today, "%y-%m-%d")
        output_csv_name = os.path.join(self.beats_path, f"{today_str}_heartbeat_log.csv")
        if os.path.exists(output_csv_name):
            self.cache_df.to_csv(output_csv_name, mode='a', header=False, index=False)
        else:
            self.cache_df.to_csv(output_csv_name, mode='w', header=True, index=False)

    def dump_JSON(self):
        """ dump a local JSON file once a day
            and keep appending the messages within that day
            into the same csv file

            Notes:
                It is risky at the moment as I overwrite each time instead
                Ideally we should compare the existing json file with the new one
                and append the new ones. If the script is re-run in the same day,
                current version would ignore the previous logs and overwrite a new one

        """
        df = get_data_strings(self.cache_df)  # the object types need to be converted for json
        curr_data = df.to_json(orient='columns')
        today = datetime.utcnow()
        today_str = datetime.strftime(today, "%y-%m-%d")
        output_json_name = os.path.join(self.beats_path, f"{today_str}_heartbeat_log.json")
        # if os.path.exists(output_json_name):
        with open(output_json_name, 'w') as file:
            #     file_data = json.load(file)
            #     # append missing keys?
            # OVERWRITE INSTEAD
            file.seek(0)
            json.dump(curr_data, file, indent=4)

    def master_csv(self):
        """ For now, also keep a csv that doesn't distinguish dates
            Append and save everything

        """
        master_csv = os.path.join(self.beats_path, f"complete_heartbeat_log.csv")
        if os.path.exists(master_csv):
            self.cache_df.to_csv(master_csv, mode='a', header=True)
        else:
            self.cache_df.to_csv(master_csv, mode='w', header=True)

    def burn_logs(self):
        """ Remove the logs after pre-decided time

        """
        today_fulldate = datetime.utcnow()
        today_str = datetime.strftime(today_fulldate, "%y-%m-%d")
        today = datetime.strptime(today_str, "%y-%m-%d")
        existing_logs = os.listdir(self.beats_path)
        if self.store:
            existing_logs = np.array([x for x in existing_logs if (x.endswith('log.json') or x.endswith('log.csv')) and
                                      ("complete_heartbeat_log.csv" not in x)])

        # take only dates
        dates_str = [i.split('/')[-1].split("_heartbeat")[0] for i in existing_logs]
        dates, files = [], []
        for d_str, logfile in zip(dates_str, existing_logs):
            try:
                dates.append(datetime.strptime(d_str, "%y-%m-%d"))
                files.append(logfile)
            except:
                continue

        time_differences = np.array([(date - today).days for date in dates])
        older_than_limit = np.where(np.abs(time_differences) > self.delete_after)
        files = np.array(files)
        log.debug(f"\t> Things will be removed; {files[older_than_limit[0]]}")
        for file in files[older_than_limit[0]]:
            os.remove(file)
            log.debug(f"\t> {file} deleted.")

    def display_table(self):
        print(f"\nCurrent cache \n{'=' * 133}\n{self.cache_df.to_markdown()}\n{'=' * 133}\n")


    def sanity_checks(self, message):
        """ check if the message will crash the server
            Check  the following
                 - detector frequencies are reasonable
                 - latencies are reasonable
                 - At least one detector is operational
        """
        issue = "no issue"
        for key in ['Received Times', 'detector_name', 'detector_status']:
            if key not in message.keys():
               issue = f" {key} is not in message keys"
        if issue == "no issue":
            if not isinstance(message['Received Times'], datetime):
                issue = f" {message['Received Times']} is not a datetime object"
            if not message['detector_status'].lower() in ['on','off']:
                issue = f" {message['detector_status']} is neither ON nor OFF"
            if not message['detector_name'] in snews_detectors:
                issue = f" {message['detector_name']} is not a valid detector"

        if issue == "no issue":
            # do not log each time
            return True
        else:
            log.error(f"\t> {message} is received at snews_hb.py but not valid.")
            return False


    def electrocardiogram(self, message):
        try:
            message["Received Times"] = datetime.utcnow()
            if self.sanity_checks(message):
                self.make_entry(message)
                self.store_beats()
                self.update_cache_csv()
                self.drop_old_messages()
                self.burn_logs()
                # if all successful, return True. Not logging each time, not to overcrowd
                return True
            else:
                return False
        except Exception as e:
            log.error(f"\t Some heartbeats didn't make it\n{e}\n")
            print(f"Something went terribly wrong \n {e}")
            return False
