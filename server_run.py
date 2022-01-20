from snews_cs.snews_coinc import CoincDecider
import hop

if __name__ == '__main__':
    print(hop.__version__)
    coinc = CoincDecider(use_local_db=True)
    coinc.run_coincidence()
