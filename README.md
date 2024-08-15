# SNEWS_Coincidence_System

Coincidence system backend for [SNEWS](https://snews2.org/) alert trigger.

![tests](https://github.com/SNEWS2/SNEWS_Coincidence_System/actions/workflows/tests.yml/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/snews-coincidence-system/badge/?version=latest)](https://snews-coincidence-system.readthedocs.io/en/latest/?badge=latest&style=for-the-badge)
[![arXiv](https://img.shields.io/badge/arXiv-2406.17743-b31b1b.svg)](https://arxiv.org/abs/2406.17743)

## How to Install

The package can be installed via `setuptools` or `poetry`. See [this page](./docs/user/installation.md) for more details.

Clone this repo and change into the directory:
```bash
git clone https://github.com/SNEWS2/SNEWS_Coincidence_System.git
cd SNEWS_Coincidence_System
```

## Usage

`snews_cs` is the main software running on the servers to initiate coincidence seraches, and trigger alerts.
 Basic usage is starting the coincidence search with the following command:
```python
snews_pt run-coincidence --no-firedrill
```

The command line tool provides information about the available commands and options:
```python
snews_pt --help
```

The heartbeat feedbacks can be tracked via the following command:
```python
snews_pt run-feedback
```
For more details and advanced usage, see the [documentation](https://snews-coincidence-system.readthedocs.io/en/latest/).

# Note for the developers
Please visit the [Developer Notes](./docs/user/Developers.md) page for more details. 
Anytime a new package needs to be introduced, please do so over poetry and update the requirements.txt file for the setuptools using poetry as described in the notes. 

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
