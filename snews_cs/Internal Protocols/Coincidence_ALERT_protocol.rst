Protocols for Publishing SNEWS Coincidence Alerts
====================================================

For SNEWS 2.0 Users

| Version 0.1
| Prepared by
| Sebastian Torres-Lara
|

.. contents:: Table of Contents

Revision History
----------------

+-----------------------+----------+----------------------+-----------+
| Name                  | Date     | Reason For Changes   | Version   |
+=======================+==========+======================+===========+
| Sebastian Torres-Lara | 04/11/21 | Initial Commit       | 0.01      |
+-----------------------+----------+----------------------+-----------+
+-----------------------+----------+----------------------+-----------+
+-----------------------+----------+----------------------+-----------+

1. Introduction
---------------
This page will go through the format the of Coincidence Alerts and when they are published to


2. Security
-----------

For the sake of testing any user with hop-auth credentials can publish an alert. However, for actual deployment only SNEWS will be able to publish alerts


3. Dependencies
---------------

Dependent on ``[unnamed Coincidence system]`` publish method (name of method) and ``snews_coinc.py``



4. Coincidence Alert Message Structure
--------------------

Note: All SNEWS messages (OBS/ALERT) are sent out as Python dictionaries.

4.1 Coincidence Tier Schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~
As of 11/02/21 has agreed to use the following schema.

#. ``_id``

#. ``num_events``

#. ``initial_nu_times``

#. ``avg_p_val``

#. ``ids``


4.1.1 Coincidence Tier Schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. ``_id`` is a string (created by built in method) that give each message sent to SNEWS a unique name, format is the following:

    SNEWS_CoincidenceTierALERT_[sent_time in SNEWS format: D/M/Y_H:M:S:mS]

    Example: ``SNEWS_CoincidenceTierALERT_02/11/21_21:57:20:19856``

#. ``num_events``  dictionary object contain the names the coincident detectors and the number of their events in the cache.

    Example:  ``'num_events':{'Detector_1':10, 'Detector_2':4, 'Detector_3':12  }``

#. ``initial_nu_times`` dictionary object contains the name coincident detectors and the time of their initial neutrino signal.

    Example: ``'num_events':{'Detector_1':'02/11/21_21:57:20:19856', 'Detector_2':'02/11/21_21:57:20:20856', 'Detector_3':'02/11/21_21:57:20:19866'}``

#. ``avg_p_val`` dictionary object contains the name coincident detectors and their average p-vales.

    Example: ``'num_events':{'Detector_1':0.74, 'Detector_2':0.67, 'Detector_3':0.56}``


#. ``ids`` list object containing the ids of the messages in the cache


5. When are Coincidence Alerts Sent Out ??
----------------------------

Short answer: Whenever the coincidence cache more than 2 coincident detectors with more than x events each.

5.1 How it Actually Works
~~~~~~~~~~~~~~~~~~~~~~~~~

#. The coincidence system runs as a stream dependent on the CoincidenceTierCache collection in _snews_db

#.  Anytime there is a new CoincidenceTier message it saves it's content

    - nu_time
    - detector_name
    - p_val

#. For a message to be coincident its nu_time must be within 10sec of the initial nu_time.

    - In the case that message arrives with a nu_time earlier than the initial it will be set as the new initial.
            - Assuming it is within 10 secs of the rest of the messages in the cache.

#. Once the cache has more than 3 coincident detector it will send out Coincidence Alert to the SNEWS subscribers.

#. As Coincidence builds (coincident detectors report more SN nu events) it will send update alerts. 









