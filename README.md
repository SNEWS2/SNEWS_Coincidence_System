[![Documentation Status](https://readthedocs.org/projects/snews-coincidence-system/badge/?version=latest)](https://snews-coincidence-system.readthedocs.io/en/latest/?badge=latest&style=for-the-badge)

|              |        |
| ------------ | ------ |
| **Docs:**    | https://snews-coincidence-system.readthedocs.io/en/latest/  |

# SNEWS_Coincidence_System
Coincidence System backend for snews alert trigger

## How to Install

Clone this repo and change into the directory:
```bash
git clone https://github.com/SNEWS2/SNEWS_Coincidence_System.git
cd SNEWS_Coincidence_System
```

Build and install the package using one of the two following methods:
### Install option 1: Poetry
If you don't already have poetry installed (check with `poetry --version`), install it:
```bash
pip3 install poetry
```

Then build and install the package
```bash
poetry build
poetry install
```

Poetry will create a virtual environment, download and install dependencies into it, and install the
`snews_cs` script.

To run in the virtual environment, you can either enter its shell directly from the terminal
(`poetry shell`, then `exit` to deactivate the environment), or you can run individual commands like
so: `poetry run <command>`.

For example, to run `snews_cs`, you can do ether:
```bash
poetry shell
# Note: Your prompt will now include text like this (snews-cs-py3.11)
# to indicate that you are in the virutal environment.

# Run the program
snews_cs --help

# Leave virtual environment; return to original shell
exit
```
or
```bash
# Poetry will execute everything after "run" in the virtual environment
poetry run snews_cs --help
```


### Install option 2: Pip and virtualenv
First install `virtualenv` if you don't already have it: `pip3 install virtualenv`.

Then create a virtual environment and install the package into it:
````bash
# Create an empty virtual environment
virtualenv venv

# Enter the virtual environment shell
source venv/bin/activate

# Install the package
pip install ./ --user
````

## Usage
The backend tools that needs to run in order for the observation messages to be cached and compared. The latest snews coincidence logic allows for efficiently searching coincidences on a pandas dataframe. The [coincidence script](https://github.com/SNEWS2/SNEWS_Coincidence_System/blob/main/snews_cs/snews_coinc.py) needs to be running for this. Furthermore, we have a [slack bot](https://github.com/SNEWS2/SNEWS_Coincidence_System/blob/main/snews_cs/snews_bot.py) which can be enabled within the coincidence script (requires channel token). This bot listens to the alert topics and publishes a slack-post as soon as it receives an alert. This way, we are aiming to reach subscribed members faster, and with a slack notification.<br>

The **observation messages** submitted to a given kafka topic using the [SNEWS Publishing Tools](https://github.com/SNEWS2/SNEWS_Publishing_Tools). The SNEWS Coincidence System listen this topic and caches all the messages submitted. These messages then assigned to different _sublists_ depending on their `neutrino_time`.

The first message in the stream makes the first sublist as `sublist=0`, and sets the coincidence window. Next, the second message is compared against the first and if the `neutrino_time` differences are less than the defined `coincidence_threshold` ([default](https://github.com/SNEWS2/SNEWS_Coincidence_System/blob/main/snews_cs/auxiliary/test-config.env) is 10sec) it is added to the same _sublist_ and an alert is triggered. This alert is sent to all subscribed users through the Kafka Alert Channels, and a slack bot message is published on the relavant SNEWS Slack channel. In case if the second message is earlier than the first one and there were no alerts triggered before (the first message was alone), then the second message is replaced as the _initial message_ of that sublist and the coincidence window is started from the neutrino time of the second message. The alert is still published.

If there are more incoming observation messages, they are compared against the _initial message_ of each sublist i.e. if the neutrino time difference of the 3rd (or later) messages are within +10 seconds they are added to the same sublist, and another alert message is published, only this time stating that it is an **"UPDATE"** on the previous alert message.

In case the new message is not coincident with the initial message of a sublist, it is assigned a new _sublist_ number e.g. `sublist=1`. Since, even though it might not coincide with the _initial message_ of the `sublist=0`, it can, in principle, still be coincident with the _later messages_ in the `sublist=0`. Thus, once a new sublist is created, we go over _all_ the messages in the cache to see if non-initial messages in other sublists coincides with this new sublist. This allows for same messages being a part of two nearby coincidence windows.

Let's look at a simple example, imagine having three detectors submitting the following messages in this order;
```python
detector1 = {'neutrino_time': "01/01/2022 12:30:00:000000"}
detector2 = {'neutrino_time': "01/01/2022 12:30:09:000000"}
detector3 = {'neutrino_time': "01/01/2022 12:30:11:000000"}
```

`detector1` sets a new sublist; `sublist=0` and opens a coincidence window. Later, `detector2` submits and it is a coincidence with `detector1` thus it is added in the same sublist and a SNEWS alert is published, everyone is happy, popping champaigns, celebrations are in order. Then, `detector3` publishes their message. The `delta time` with the _initial message_ of the `sublist=0` is 11 seconds, thus it does not enter this sublist. It creates a new sublist `sublist=1`. Since there is a new sublist created, we go back and look if any of the existing messages would also satisfy the coincidence condition with this sublist. Indeed, the `detector2` has a `delta time` of -2 seconds! Since this is an _earlier message_ we assign `detector2` to be the _initial message_ of `sublist=2` and keep the `detector3` as a second message in sublist with a `delta time=+2`. Finally, we sent another message with this new coincidence sublist. At the end, there will be two alerts corresponding to `sublist=0` and `sublist=1`. Notice, if there were a 4th message at a much later time, it would also create a `sublist=3` and look through the cache. However, since nothing would match, it would sit alone and wait for more messages to come without triggering any alert.

In reality, Supernova time window is expected to be much smaller than 10 seconds, thus it is already a conservative time boundary. However, we wanted to have a stable and bullet proof logic that could also prove useful during stres tests and firedrills.

All the observation messages are kept in cache for 24 hours after their `neutrino_time` and unless they are involved in any coincidence they are discarded. These messages together with the triggered alert messages are also locally stored in the Purdue servers through a Mongo Database connection.


# Example Usage
# API



# CLI

```bash
(venv) User$: snews_cs run-coincidence
(venv) User$: snews_cs run-slack-bot
```

Details about the commands can also be displayed via passing `--help` flag. The slack bot requires authentication through a token, contact Melih Kara or Sebastian Torres-Lara if needed.

# SNEWS Detector List

* “Baksan”
* “Borexino”
* “DS-20K”
* “DUNE”
* “HALO”
* “HALO-1kT”
* “Hyper-K”
* “IceCube”
* “JUNO”
* “KM3NeT”
* “KamLAND”
* “LVD”
* “LZ”
* “MicroBooNe”
* “NOvA”
* “PandaX-4T”
* “SBND”
* “SNO+”
* “Super-K”
* “XENONnT”
