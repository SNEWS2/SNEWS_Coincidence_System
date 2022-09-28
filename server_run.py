from snews_cs.snews_coinc import CoincDecider
import hop, os

# from  snews_cs import  _version

if __name__ == '__main__':
    print(f'hop version: {hop.__version__}')
    # print(f'SNEWS CS version: {_version.__version__}')
    server_tag = os.getenv('hostname')
    coinc = CoincDecider(use_local_db=True, drop_db=True, firedrill_mode=False, send_email=True, server_tag=server_tag)
    coinc.run_coincidence()
