import pymongo
from . import snews_utils
import os


class Storage:
    """
    This class sets up the SNEWS database using Mongo DB

    Parameters
    ----------
    env : `str`, optional 
        Path to env file, defaults to './auxlilary/test-config.env'
    drop_db : `bool`, optional
        drops all items in the DB every time Storage is initialized, defaults to False
    use_local : `bool`, optional 
        Tells Storage to set up a local DB, defaults to False.

    """

    def __init__(self, env=None, drop_db=True, use_local=False):
        snews_utils.set_env(env)
        self.mgs_expiration = int(os.getenv('MSG_EXPIRATION'))
        self.coinc_threshold = int(os.getenv('COINCIDENCE_THRESHOLD'))
        self.mongo_server = os.getenv('DATABASE_SERVER')

        if use_local:
            self.client = pymongo.MongoClient('mongodb://localhost:27017/', replicaset='rs0')
        else:
            self.client = pymongo.MongoClient(self.mongo_server)

        self.db = self.client.snews_db
        self.all_mgs = self.db.all_mgs
        self.false_warnings = self.db.false_warnings
        self.sig_tier_cache = self.db.sig_tier_cache
        self.time_tier_cache = self.db.time_tier_cache
        self.coincidence_tier_cache = self.db.coincidence_tier_cache
        self.coincidence_tier_alerts = self.db.coincidence_tier_alerts
        self.time_tier_alerts = self.db.time_tier_alerts
        self.sig_tier_alerts = self.db.sig_tier_alerts

        if drop_db:
            # kill all old colls
            self.all_mgs.delete_many({})
            self.coincidence_tier_cache.delete_many({})
            self.coincidence_tier_alerts.delete_many({})
            self.false_warnings.delete_many({})
            self.time_tier_cache.delete_many({})
            self.sig_tier_cache.delete_many({})
            self.time_tier_alerts.delete_many({})
            self.sig_tier_alerts.delete_many({})
            # drop the old index
            self.all_mgs.drop_indexes()
            self.coincidence_tier_cache.drop_indexes()
            self.false_warnings.drop_indexes()
            self.coincidence_tier_alerts.drop_indexes()
            self.time_tier_cache.drop_indexes()
            self.sig_tier_cache.drop_indexes()
            self.time_tier_alerts.drop_indexes()
            self.sig_tier_alerts.drop_indexes()
            # set index
            self.all_mgs.create_index('sent_time')
            self.coincidence_tier_cache.create_index('sent_time', expireAfterSeconds=self.mgs_expiration)
            self.sig_tier_cache.create_index('sent_time', expireAfterSeconds=self.mgs_expiration)
            self.time_tier_cache.create_index('sent_time', expireAfterSeconds=self.mgs_expiration)
            self.false_warnings.create_index('sent_time')
            self.coincidence_tier_alerts.create_index('sent_time')
            self.sig_tier_alerts.create_index('sent_time')
            self.time_tier_alerts.create_index('sent_time')

        self.coll_list = {
            'CoincidenceTier': self.coincidence_tier_cache,
            'SigTier':self.sig_tier_cache,
            'TimeTier':self.time_tier_cache,
            'FalseOBS': self.false_warnings,
            'CoincidenceTierAlert': self.coincidence_tier_alerts,
            'SigTierAlert': self.sig_tier_alerts,
            'TimeTierAlert': self.time_tier_alerts,
        }

    def insert_mgs(self, mgs):
        """ This method inserts a SNEWS message to its corresponding collection
        
        Parameters
        ----------
        mgs : `dict`
            dictionary of the SNEWS message

        """
        mgs_type = mgs['_id'].split('_')[1]
        specific_coll = self.coll_list[mgs_type]
        specific_coll.insert_one(mgs)
        self.all_mgs.insert_one(mgs)

    def get_all_messages(self, sort_order=pymongo.ASCENDING):
        """ Returns a list of all messages in the 'all-messages' collection

        Parameters
        ----------
        sort_order : `object`, optional
            default to `pymongo.ASCENDING`

        Returns
        -------
        result : `list`
            A list containing all items inside 'All-Messages'

        """
        return self.all_mgs.find().sort('sent_time', sort_order)

    def get_coincidence_tier_cache(self, sort_order=pymongo.ASCENDING):
        """ Returns a list of all messages in the 'cache' collection

        Parameters
        ----------
        sort_order : `object`, optional
        default to `pymong.ASCENDING`

        Returns
        -------
        result : `list`
            A list containing all items inside 'Coincidence Tier Cache'
                    
        """
        return self.coincidence_tier_cache.find().sort('sent_time', sort_order)

    def get_false_warnings(self, sort_order=pymongo.ASCENDING):
        """ Returns a list of all messages in the 'cache' collection

        Parameters
        ----------
        sort_order : `object`, optional
        default to `pymong.ASCENDING`

        Returns
        -------
        result : `list`
            A list containing all items inside 'Coincidence Tier Cahce'
            
        """
        return self.false_warnings.find().sort('sent_time', sort_order)

    def empty_false_warnings(self):
        """ Returns True of if false warnings is empty

        """
        if self.false_warnings.count() == 0:
            return True
        else:
            return False

    def empty_coinc_cache(self):
        """ Returns True of if coincidence cache is empty

        """
        if self.coincidence_tier_cache.count() <= 1:
            return True
        else:
            return False

    def purge_cache(self, coll):
        """ Erases all items in a collection

        Parameters
        ----------
        coll : `str` 
            name of collection

        """
        self.coll_list[coll].delete_many({})

    def get_alert_collection(self, which_tier):
        """Gives a Mongo cursor for a specifc alert collection

        Parameters
        ----------
        which_tier : 'str'
            Name of OBS tier

        Returns
        -------
        collection cursor: 'mongo cursor object'
            An ordered list of all documents inside the collection.

        """
        sort_order = pymongo.ASCENDING
        return self.coll_list[f'{which_tier}Alert'].find().sort('sent_time', sort_order)

