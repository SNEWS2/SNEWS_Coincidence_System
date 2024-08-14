[![Documentation Status](https://readthedocs.org/projects/snews-coincidence-system/badge/?version=latest)](https://snews-coincidence-system.readthedocs.io/en/latest/?badge=latest&style=for-the-badge)
[![arXiv](https://img.shields.io/badge/arXiv-1234.56789-b31b1b.svg)](https://arxiv.org/abs/2406.17743)

|              |        |
| ------------ | ------ |
| **Docs:**    | https://snews-coincidence-system.readthedocs.io/en/latest/  |

# SNEWS_Coincidence_System
Coincidence System backend for snews alert trigger

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
