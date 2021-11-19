==========
Hop-Comms
==========

.. contents::
   :local:


An interface for participating observatories to easily interact with the SNEWS server. In particular, :code:`hop-comms` allow user to 

  * Submit heartbeat messages in the background with a given schedule.
  * Submit observation messages with a pre-defined detector specific fields.
  * Subscribe to alert channels and listen SNEWS for coincidence alerts. 

Hop-Pub
-------

This module contains the methods for publishing messages to the snews servers.

.. code:: python

    from hop_pub import Publish_Observation
    publisher = Publish_Observation(detector='XENONnT')

The script automatically generates a default observation message for the given detector with detector's unique ID and location, and inserting the time of execution as the observation time. These fields can be displayed, modified and expanded.

.. code:: python

    publisher.display_message()
    publisher.message_dict['content'] = 'This is a modified content'


Hop-Sub
-------

This module contains the methods for subscribing to the alert messages from the snews servers.