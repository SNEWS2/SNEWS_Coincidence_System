"""
Melih Kara 26/06/2021

Script to create detector file.
New detectors can be added here.
"""

import json, os
from collections import namedtuple

Detector = namedtuple("Detector", ["name", "id", "location"])
detectors = {
        "TEST" :      Detector("TEST", 0,"TESTloc"),
        "Super-K" :   Detector("Super-K", 1, "loc Super-K"),
        "Hyper-K" :   Detector("Hyper-K", 2, "loc Hyper-K"),
        "SNO+" :      Detector("SNO+", 3, "loc SNO+"),
        "KamLAND" :   Detector("KamLAND", 4, "loc KamLAND"),
        "LVD" :       Detector("LVD", 5, "loc LVD"),
        "ICE" :       Detector("ICE", 6, "loc ICE"),
        "Borexino" :  Detector("Borexino", 7, "loc Borexino"),
        "HALO-1kT" :  Detector("HALO-1kT", 8, "loc HALO-1kT"),
        "HALO" :      Detector("HALO", 9, "loc HALO"),
        "NOvA" :      Detector("NOvA", 10, "loc NOvA"),
        "KM3NeT" :    Detector("KM3NeT", 11, "loc KM3NeT"),
        "Baksan" :    Detector("Baksan", 12, "loc Baksan"),
        "JUNO" :      Detector("JUNO", 13, "loc JUNO"),
        "LZ" :        Detector("LZ", 14, "loc LZ"),
        "DUNE" :      Detector("DUNE", 15, "loc DUNE"),
        "MicroBooNe" :Detector("MicroBooNe", 16, "loc MicroBooNe"),
        "SBND" :      Detector("SBND", 17, "loc SBND"),
        "DS-20K" :    Detector("DS-20K", 18, "loc DS-20K"),
        "XENONnT" :   Detector("XENONnT", 19, "loc XENONnT"),
        "PandaX-4T" : Detector("PandaX-4T", 20, "loc PandaX-4T"),
    }

with open(os.path.dirname(__file__) + "/detector_properties.json", 'w') as outfile:
    json.dump(detectors, outfile, indent=4, sort_keys=True)

print('detector_properties.json file created!')