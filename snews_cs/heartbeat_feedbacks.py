
import json, os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from time import sleep
from .core.logging import getLogger
from .cs_email import send_warning_mail, send_feedback_mail

log = getLogger(__name__)

contact_list_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'auxiliary/contact_list.json'))
with open(contact_list_file) as file:
    contact_list = json.load(file)

# Check if detector name is in registered list.
detector_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'auxiliary/detector_properties.json'))
with open(detector_file) as file:
    snews_detectors = json.load(file)
snews_detectors = list(snews_detectors.keys())

# this csv file is mirroring the existing heartbeat cache
beats_path = os.path.join(os.path.dirname(__file__), "../beats")
csv_path = os.path.join(beats_path, f"cached_heartbeats_mirror.csv")


class FeedBack:
    """ Once every minute, check the HB of each detector
        If the last heartbeat is from longer than usual, send an email
        Once every user-defined time interval, send a plot with latency and frequency statistics

    """
    def __init__(self):
        self.detectors = snews_detectors
        self.last_feedback_time = dict()
        for k in self.detectors:
            self.last_feedback_time[k] = datetime(2022, 1, 1)
        self.day_in_min = 1440
        self.running_min = 0
        self.contact_intervals = get_intervals()
        self.contact_running_hours = {k:0 for k in self.contact_intervals.keys()}

    def __call__(self):
        """ Continuously run and check expected heartbeats every minute
            Also, check if the detectors requested feedbacks
            create and send feedbacks with the desired time intervals
        """
        while True:
            # run every minute
            sleep(60)
            try:
                df = pd.read_csv(csv_path, parse_dates=['Received Times'], )
            except FileNotFoundError:
                log.error(f"{csv_path} does not exist yet! Maybe `snews_cs run-coincidence` is not invoked?")
                while not os.path.isfile(csv_path):
                    sleep(60)
                df = pd.read_csv(csv_path, parse_dates=['Received Times'], )
                log.debug(f"OK {csv_path} found! Moving on")

            self.control(df) # check if a detector is taking longer than usual (mean+3*sigma>)
            self.running_min += 1
            print(f"[DEBUG] >>>>> Running minute: {self.running_min}")
            # every hour, reset the minute counter, increase hour counter
            # and check if it has been feedback time for any detector.
            if (self.running_min % 60) == 0:
                self.running_min = 0  # reset the counter

                for detector, interval in self.contact_intervals.items():
                    self.contact_running_hours[detector] += 1
                    if (self.contact_running_hours[detector] % interval) == 0:
                        # check frequencies, and send feedback
                        self.check_frequencies(df, detector)
                        # Reset the running hours
                        self.contact_running_hours[detector] = 0

            # ### temp
            # for detector, interval in self.contact_intervals.items():
            #     print(">>> Checking frequencies")
            #     self.check_frequencies(df, detector)
            # ### temp



    def control(self, df):
        """ Check the current cache, check if any detector
            missed a beat

        """
        # get the heartbeats of this detector from last 24 hours
        last24hours = (datetime.utcnow() - timedelta(hours=24))
        data = df[df['Received Times'] > last24hours]
        data.sort_values('Received Times', inplace=True)

        for detector in data['Detector'].unique():
            detector_df = data.query('Detector==@detector')
            if len(detector_df) < 5:
                # not enough statistics, skip.
                print("[DEBUG] >>>>> len=",len(detector_df), "Not enough")
                continue
            # check if a heartbeat is skipped
            self.check_missed_beats(detector_df, detector)

    def check_missed_beats(self, df, detector):
        """ Check if a heartbeat is skipped

        """
        print("\n[DEBUG] >>>>> Checking if beat skipped")
        # get the computed delays between 2 consecutive hb
        mean = np.mean(df['Time After Last'])
        std = np.std(df['Time After Last'])

        last_hb = df['Received Times'].values[-1] # this is a numpy.datetime
        last_hb = pd.to_datetime(last_hb)         # we have to convert it to datetime.datetime
        since_lasthb = datetime.utcnow() - last_hb
        print(f"[DEBUG] >>>>> mean:{mean:.2f}, std:{std:.2f}, trigger at {mean + 3 * std:.2f}")
        print(f"[DEBUG] >>>>> Delay since last: {since_lasthb.total_seconds():.2f}")
        if since_lasthb > timedelta(seconds=(mean + 3 * std)):
            # something is wrong!
            if last_hb == self.last_feedback_time[detector]:
                # this warning has already been sent! Skip it
                return None
            expected_hb = last_hb + timedelta(seconds=float(mean))  # +/- std
            text = f" Your ({detector}) heartbeat frequency is every {mean:.2f}+/-{std:.2f} sec\n" \
                   f" Expected a heartbeat at {expected_hb.isoformat()} +/- {std:.2f} sec\n" \
                   f" Since last heartbeat there has been {since_lasthb.total_seconds():.2f} sec\n" \
                   f" Is everything alright? Do you wanna talk about it?"
            print("[DEBUG] >>>>> \n",text, "\n")
            # send warning to detector
            # send_warning_mail(detector, text)
            self.last_feedback_time[detector] = last_hb
        return None


    def check_frequencies(self, df, detector):
        """ Create a plot with latency and heartbeat frequencies
        """
        now_str = datetime.utcnow().strftime("%Y-%m-%d_%HH%MM")
        mean = np.mean(df['Time After Last'])
        std = np.std(df['Time After Last'])
        last_hb = df['Received Times'].values[-1]  # this is a numpy.datetime
        last_hb = pd.to_datetime(last_hb)  # we have to convert it to datetime.datetime
        text = f" Your heartbeat frequency is every {mean:.2f}+/-{std:2f} sec\n" \
               f" The last heartbeat received at {last_hb} \n" \
               f" The received heartbeat frequency, together with the computed latency" \
               f" is plotted, and sent in the attachment.\n" \
               f" The next feedback will be send in {self.contact_intervals[detector]} hours."
        attachment = f"{detector}_{now_str}.png"
        print("plotting...")
        print(df, "\n\n", detector, "\n\n", attachment)
        plot_beats(df, detector, attachment)  # create a plot to send
        # send_feedback_mail(detector, attachment, text)
        print("[DEBUG] >>>>> \n",text, "\n")
        print("Check the created plot-->", os.path.join(beats_path, attachment))

    def check_enough_detectors(self):
        pass

def get_intervals():
    """ Read the contact list and parse
        Expected format "24H" with 'H' indicating hour
        if None, no feedback will be provided
    """
    interval_dict = dict()
    for detector in contact_list.keys():
        interval = contact_list[detector]['feedback interval']
        if interval == 'None':
            continue
        else:
            try:
                hour_interval = interval.split("H")[0]
                interval_dict[detector] = int(hour_interval)
            except Exception as e:
                log.warning(f"\t> {detector} has a feedback interval which is in wrong format"
                            f"\t> Expected format like-'24H' got {interval}\n{e}")
    return interval_dict


def plot_beats(df, detector, figname):
    fig, ax = plt.subplots(figsize=(12, 3))
    latency = pd.to_timedelta(df['Latency'].values).total_seconds()
    received_times = df['Received Times']
    time_after_last = df['Time After Last'].astype(float)

    mean = np.mean(time_after_last)
    std = np.std(time_after_last)

    ax.fill_between(received_times, mean - 3 * std, mean + 3 * std, alpha=0.5, color='yellow')
    ax.fill_between(received_times, mean - std, mean + std, alpha=0.5, color='green')
    ax.axhline(mean)
    colors = ['g' if i == 'ON' else 'r' for i in df['Status']]
    # ax.plot_date(received_times, time_after_last, c=colors)
    ax.scatter(received_times, time_after_last, marker='o', c=colors, ec='k', zorder=2)
    ax.set_xlabel("Received Times")
    ax.set_ylabel("Seconds after last hb")
    ax2 = ax.twinx()

    ax2.scatter(received_times, latency, ls='--', c='none', ec='magenta')
    ax2.set_ylabel('Latency [sec]\nReceived at server - Sent from local', color='magenta')
    ax.set_title(f"HB data for {detector}, last 24hr")
    plt.savefig(os.path.join(beats_path, figname))