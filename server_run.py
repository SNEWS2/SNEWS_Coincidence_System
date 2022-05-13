from snews_cs.snews_coinc import CoincDecider
import hop
# from  snews_cs import  _version

if __name__ == '__main__':
    print(f'hop version: {hop.__version__}')
    # print(f'SNEWS CS version: {_version.__version__}')
    coinc = CoincDecider(use_local_db=True, drop_db=True, firedrill_mode=False)
    coinc.run_coincidence()
