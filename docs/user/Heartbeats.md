# Heartbeat Handling

`snews_cs` not only tracks the detector messages to sarch for coincidences and issue alerts but it also tracks the heartbeats of each detector and provides feedback.

There are several scripts for registering, analysing and executing some commands on heartbeats. 
However, the main idea of heartbeat tracking is to make sure we can receive the observation message from an experiment in case of a supernova. Therefore, it is crucial to use the same stream for recording the heartbeats. 

The center of our SNEWS Coincidence System is the main coincidence script `snews_coinc.py` which subscribes to the relevant Kafka topic and listens all the messages.
While the main purpose of this script is to run a coincidence algorithm, decide if the neutrino times from different detectors coincide, and trigger a SNEWS alert, it can do more than that.

Simultaneously, coincidence instance also creates a heartbeat cache from `snews_hb.Heartbeat()`. The messages in the stream are checked for their contents using `cs_remote_commands.py`, this decides whether the message in the stream is in 
SNEWS format and what is its intended use. 

The messages with `*_CoincidenceTier_*` in their `_id` field are added to cache and run through the coincidence logic. All the other 
type messages (heartbeat, feedback, retraction, connection test, cache reset) are handled in their respective functions or scripts.

Once the coincidence script sees a message with `_id` field containing `*_Heartbeat_*` it registers this to the _heartbeat cache_ in `snews_hb.Heartbeat()` instance, then moves on. Below is an example of a received heartbeat message

```bash
{'_id': '19_Heartbeat_2023-01-13T09:23:24.583978', 
'detector_name': 'XENONnT', 
'machine_time': '2023-01-13T09:23:24.583978', 
'detector_status': 'ON', 
'meta': {}, 
'schema_version': '1.2.0', 
'sent_time': '2023-01-13T09:23:24.583978'}
```

## snews_hb.Heartbeat()

This instance is responsible for controlling the heartbeat message, registering it to cache, storing for given time, and removing older beats. Since the heartbeats can be sensitive information, the beats are stored for 7 days in csv files and deleted, and also kept in cache as a `pandas.DataFrame` for 24 hours.

The script is therefore adds new beats and removes older than 24h beats from its cache, and stores beats daily for 7 days, and deletes stored-beats that are older than 7 days.

For feedback purposes, the dynamic cache is also mirrored in a csv file by continuously adding new beats and removing old ones, this cache is called `beats/cached_heartbeats_mirrored.csv`.

It also appends a `"Received Time"` time stamp for each beat and comparing that with `"Sent Time""`, computes and appends a `"Latency"` field, which is also used for Feedbacks.

All the parameters of `snews_hb.Heartbeat()` instance are adjustable, i.e. the cache and store durations.


## Feedback Mechanism

We do not just want to store the heartbeats for a while and delete but actually perform some checks and provide feedback.

There are two types of feedback we provide;
- If the detector has skipped a beat based on their past frequencies.
- If the authorized detector user(s) request heartbeat (frequency, latency, status) feedback

In order to track these we initiate a second persistent script `Heartbeat_feedbacks.FeedBack()` which can be called with CLI `snews_cs run-feedback`.

### Skipped Beats
This script runs parallel to the coincidence script (`snews_coinc.py`) and executes some tasks every 1 minute (adjustable).

Every minute, it opens the `cached_heartbeats_mirrored.csv` file and for each detector investigates the `"Time Since Last Beat"`. 
If there are more than 5 entries for a given detector, it calculates a simple mean and standard deviation of the beat frequency.
Based on these it expects the next beat in $\mu \pm 3\sigma$, if this time has passed and no beats is registered at the time of the check, it fires a warning to the user.

**Example**<br>
Say the detector A sends heartbeats every 5 minutes, `(0,5,10.5,15,19.5,25)` and `Feedback()` checks these every minute. 
After 20 minutes, it is able to calculate a mean and estimates the next beat.<br>

> At $t=20$min, mean("time after last") = (5 + 5.5 + 4 + 4.5)/4 = 4.75 <br>
> Expect next beat in 4.75 +/- 3*0.56 minutes = (3.07, 6.43) <br>
> Since the last beat was at $t=19.5$ it expects the next one at (22.57, 25.93)

The script runs every minute and finds the same expected beat range until $t=25$, then it updates its estimation.
It would only fire a warning if at $t=26$ it wouldn't see a new beat. Let's look at that now.<br>

Now imagine the next beat came later than usual; `(0,5,10.5,15,19.5,25,33.5)` and now the time is $t=26$ so we 
ran the feedback and calculated when to expect the next beat;
> Time After Last: (5, 5.5, 4.5, 4.5, 5.5), mean=5, std=0.45 <br>
> Expected next signal = 25 + (5 +/- 3*0.45) =  (28.65, 31.35)

Now the script keeps checking every minute, and since there is no new messages, it doesn't update, and it 
always expects the next beat between (28.65, 31.35), when the script checks again at $t=32$ and still doesn't see a new message
and since the expected range is also exceeded, it decides that something went wrong and creates a warning message.

It uses the pre-compiled contact persons for each experiment to send an email with the following format;

> "Dear {detector}, " \
  " Your ({detector}) heartbeat frequency is every {mean:.2f}+/-{std:.2f} sec" \
  " Expected a heartbeat at {Last Beat} + ({mean} +/- {std:.2f}) sec" \
  " Since last heartbeat there has been {since_lasthb.total_seconds()} sec" \
  " Is everything alright? Do you wanna talk about it?" \
  " Provided by the SNEWS server. \
> "--Cheers "

### Requested Feedbacks

At any given time we keep the heartbeats from the past 24h in the cache. We provide a tool with the 
`snews_pt` for requesting personal feedback on your detector's heartbeats. 

The idea is to provide the user with their heartbeat statistics from past 24 hours. This includes 
the time distribution of the heartbeats registered at the server, the latency between their machines and the server using 
the `"Sent"` and `"Received"` timestamps, and also regarding the detector status they provided.

To protect users from vulnerabilities we intend to protect this communication between the server and user the best way we can.
For that we send the feedback via e-mail to only the users with approved email addresses for given detector.

For the time being, a temporary asymmetric encryption is being tested. However, we would like to make use of the Kafka's authentication system to 
associate each user with their detector and authenticate them.



































