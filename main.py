import json
import sys, os
from dejavu import Dejavu
from dejavu.logic.recognizer.file_recognizer import FileRecognizer
import pandas as pd
import datetime
from collections import defaultdict

# load config from a JSON file (or anything outputting a python dictionary)
audio_ext = ".mp3"


def recursive_process_directory(path, depth, function, *function_args):
    if depth == 0:
        for file in sorted(os.listdir(path)):
            if file[0] == ".":
                print(file)
                continue
            function(os.path.join(path, file), *function_args)
    else:
        for file in sorted(os.listdir(path)):
            if os.path.isdir(os.path.join(path, file)):
                if file[0] == ".":
                    print(file)
                    continue
                recursive_process_directory(
                    os.path.join(path, file), depth - 1, function, *function_args
                )


djv = Dejavu({})


def hash_csv(path):
    import time

    global djv
    start_time = time.time()
    csv_path = os.path.join(path, "cm_detection_results.csv")
    out_path = os.path.join(path, "cm_detection_results_hashed.csv")
    if not os.path.exists(csv_path):
        print(f"CSV file {csv_path} does not exist")
        return
    df = pd.read_csv(csv_path)
    foldername = [x for x in os.listdir(path) if "." not in x][0]
    df = pd.read_csv(csv_path)

    def helper(row):
        file_path = os.path.join(
            path,
            foldername,
            "temp",
            f"{datetime.datetime.strptime(row['start_datetime'], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y%m%d_%H%M%S')}{audio_ext}",
        )
        return djv.fingerprint_file_to_hash(file_path)

    df["hashes"] = df.apply(helper, axis=1)
    df.to_csv(out_path, index=False)
    print(f"hashed {len(df)} rows in {time.time() - start_time} seconds")


if __name__ == "__main__":
    # df_cm = process_ground_truth()
    # cm_by_time = defaultdict(list)
    # for idx, row in df_cm.iterrows():
    #     cm_by_time[row["start_datetime"]].append(row["content"])
    # # recursive_process_directory(sys.argv[1], 5, fingerprint_csv)
    # # recursive_process_directory(sys.argv[1], 5, update_csv)
    import time

    start_time = time.time()
    recursive_process_directory(sys.argv[1], 5, hash_csv)
    print(f"hashed in {time.time() - start_time} seconds")
