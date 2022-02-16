# SNEWS_Coincidence_System
Coincidence System backend for snews alert trigger

## How to Install

First you need to clone this repo. In your terminal run the following:

````bash 
git clone https://github.com/SNEWS2/SNEWS_Coincidence_System.git
cd SNEWS_Coincidence_System
pip install ./ --user
````

The backend tools that needs to run in order for the observation messages to be cached and compared. The latest snews coincidence logic allows for efficiently searching coincidences on a pandas dataframe. The [coincidence script](https://github.com/SNEWS2/SNEWS_Coincidence_System/blob/main/snews_cs/snews_coinc_v2.py) needs to be running for this. Furthermore, we have a [slack bot](https://github.com/SNEWS2/SNEWS_Coincidence_System/blob/main/snews_cs/snews_bot.py) that can also be initiated (requires channel token). This bot listens to the alert topics and publishes a slack-post as soon as it receives an alert. This way, we are aiming to reach subscribed members faster, and with a slack notification.<br>

# Example Usage
# API



# CLI

```bash
(venv) User$: snews_cs run-coincidence
(venv) User$: snews_cs run-slack-bot
```

Details about the commands can also be displayed via passing `--help` flag. The slack bot requires authentication through a token, contact Melih Kara or Sebastian Torres-Lara if needed.

