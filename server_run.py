from snews_cs.snews_coinc_v2 import CoincidenceDistributor
import hop
from socket import gethostname

# from  snews_cs import  _version

if __name__ == '__main__':
    print(f'hop version: {hop.__version__}')
    # print(f'SNEWS CS version: {_version.__version__}')
    server_tag = gethostname()
    coinc = CoincidenceDistributor(use_local_db=True, drop_db=True, firedrill_mode=False, send_email=True, server_tag=server_tag)
    coinc.run_coincidence()
