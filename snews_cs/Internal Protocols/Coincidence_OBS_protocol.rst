Protocols for Publishing to SNEWS Observation Tiers
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
| Sebastian Torres-Lara | 02/11/21 | Initial Commit       | 0.01      |
+-----------------------+----------+----------------------+-----------+
+-----------------------+----------+----------------------+-----------+
+-----------------------+----------+----------------------+-----------+

1. Introduction
---------------

This page aims to explain the structure and use of publishing to SNEWS’s Coincidence Tier.


2. Security
-----------

Any user with hop credentials (see link to `hop auth tutorial <https://github.com/SNEWS2/hop-SNalert-app/blob/demo/tutorial/snews-dev-tutorial.md#account-setup>`_ ) can publish to SNEWS’s Observation tiers


3. Dependencies
---------------

The methods in this page are dependent on the SNEWS_PT package (link to Pypip page)


4. Message Structure
--------------------

Note: All SNEWS messages (OBS/ALERT) are sent out as Python dictionaries. 


4.1 Coincidence Tier Schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~
As of 11/02/21 has agreed to use the following schema.

#. ``_id``

#. ``detector_name``

#. ``nu_time``

#. ``machine_time``

#. ``p_val``


4.2 Notes on Message Items
~~~~~~~~~~~~~~~~~~~~~~~~~~

#. ``_id`` is a string (created by built in method) that give each message sent to SNEWS a unique name, format is the following:
	  
    [detector_id_number]_[observation_type]_[sent_time in SNEWS format: D/M/Y_H:M:S:mS]
	  
    Example: ``18_CoincidenceTier_02/11/21_21:57:20:19856``
    
#. ``detector_name`` a string used for the detector name. See ``detector_config.json`` for SNEWS appropriate name formats. 

#. ``nu_time`` a string used for the neutrino time that the detector saw a neutrino signal. 

#. ``machine_time`` a string used to record the current time of detector. 

#. ``p_val`` float number representing the p-value of the neutrino event. 



5. When to Publish to Coincidence Tier ?? 
-----------------------------------------

Note: See `SNEWS_PT Publishing tutorial <>`_ for a guide on how use the publishing method. 

A detector/user should publish to Coincidence Tier whenever their SN neutrino trigger goes off (i.e., one message per event) 


