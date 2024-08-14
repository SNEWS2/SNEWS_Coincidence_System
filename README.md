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
[Poetry](https://python-poetry.org/) makes it easy to manage and install dependencies, build projects, and publish packages to PyPI.


If you don't already have poetry installed (check with `poetry --version`), install it:
```bash
pip3 install poetry
```

Poetry relies on the `pyproject.toml` file for project-related stuff, so be sure to issue the following poetry commands from the same directory with that file (i.e., the root directory of this codebase).

The below command will install the package and all its dependicies. When run for the first time, poetry will create a new virtual environment on your filesystem (but not in your working directory). Poetry will know where to find this virtual environment in the future; there is no need to source it.

> Note: By default, Poetry will create the virtual environment based on your locally installed version of python (i.e., `$(which python)`). If you would like to specify a specific version of python, you can run the optional command listed below.

```bash
# Optional: Tell Poetry which version of python you want to use by pointing to the appropriate binary.
#
# poetry env use <path-to-python>
# ex. poetry env use /usr/local/bin/python3.9
# ex. poetry env use $(which python3.9)

poetry install
```

To run in the virtual environment, you can either enter its shell directly from the terminal
(`poetry shell`, then `exit` to deactivate the environment), or you can run individual commands like
so: `poetry run <command>`.

For example, to run `snews_cs`, you can do ether:
```bash
# Poetry will execute everything after "run" in the virtual environment
poetry run snews_cs --help
```
or (if you still want to activate/deactivate your venv)

```bash
poetry shell
# Note: Your prompt will now include text like this (snews-cs-py3.11)
# to indicate that you are in the virutal environment.

# Run the program
snews_cs --help

# Leave virtual environment; return to original shell
exit
```

#### Poetry notes for developers
**Q: Why Poetry? A: Dependency hell.**

`requirements.txt` mixes top-level dependencies and lower-level dependencies in a single file. Want to upgrade the version of a middle-level dependency that is compatible with multiple python versions? Good luck! Try this command:
```shell
poetry show --tree # Show dependencies and sub-dependencies

# EXAMPLE OUTPUT:
# flake8 6.1.0 the modular source code checker: pep8 pyflakes and co
# ├── mccabe >=0.7.0,<0.8.0
# ├── pycodestyle >=2.11.0,<2.12.0
# └── pyflakes >=3.1.0,<3.2.0
# hop-client 0.8.0 A pub-sub client library for Multi-messenger Astrophysics
# ├── adc-streaming >=2.1.0
# │   ├── authlib *
# │   │   └── cryptography >=3.2
# │   │       └── cffi >=1.12
# │   │           └── pycparser *
```

**Q: Wait, how do I activate/deactivate my environment? A: Don't.**

If you're coming from the world of `virtualenv`, you're used to creating your env in a subdirectory of your projcet, sourcing the `venv/bin/activate` file, and using `deactivate` when you're done. But why polute your working directory? Poetry will create the venv elsewhere. When you use `poetry run <command>` it will run your `<command>` in the venv for you. When you say `poetry install`, it installs to the venv for you. No need to source/deactivate it.

**Q: How do I add a new dependcies? A: Similar to pip, but a bit different.**

```shell
# The following command is equivalent to:
# "source venv/bin/activate; pip install numpy; pip freeze > requirements.txt; deactivate"
poetry add numpy
```

**Q: But how do I know what venv I'm using? How do I delete it and start fresh?" A: Poetry can answer that for you.**

Check which environments are available with `poetry env list`. It will tell you which one is currently "active" ("active" != sourced; "active" == the one poetry will use when you issue poetry from that directory). If you want to switch to a specific environment, use `poetry env use <name from poetry env list>`. If you want to remove a virtual envirnoment so you can start fresh, use `poetry env remove <name from poetry env list>`.

**Q: So this replaces `pip` and `requirements.txt`? A: Not necessarily.**

This takes a lot of hassle out of maintaining `requirements.txt`, but it can co-exist so that folks can choose which method they prefer for installing dependencies. But maintaining 2 files that define dependencies sounds like a lot. Poetry can generate a `requirements.txt` like so:
```shell
poetry export --without-hashes --format=requirements.txt > requirements.txt
```

**Q: How do I publish this package to PyPI or some other index? A: Great!**

```shell
poetry build # Builds sdist and wheel
poetry publish # Credentials required
```

**Q: I still have questions about how to use poetry... A: you can get command help.**

Type `poetry` into any shell to see a manual of commands.

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

The first message in the stream makes the first sublist as `sublist=0`, and sets the coincidence window. Next, the second message is compared against the first and if the `neutrino_time` differences are less than the defined `coincidence_threshold` ([default](https://github.com/SNEWS2/SNEWS_Coincidence_System/blob/main/snews_cs/etc/test-config.env) is 10sec) it is added to the same _sublist_ and an alert is triggered. This alert is sent to all subscribed users through the Kafka Alert Channels, and a slack bot message is published on the relavant SNEWS Slack channel. In case if the second message is earlier than the first one and there were no alerts triggered before (the first message was alone), then the second message is replaced as the _initial message_ of that sublist and the coincidence window is started from the neutrino time of the second message. The alert is still published.

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
