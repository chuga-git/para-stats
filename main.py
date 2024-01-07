from para_stats import init_script
import sys

if __name__ == "__main__":
    # TODO: debug cmd line arguments, get rid of this later 
    if len(sys.argv) > 1:
        round_id = int(sys.argv[1])
    else:
        round_id = 38723 # debug default gets us one batch

    init_script(round_id)