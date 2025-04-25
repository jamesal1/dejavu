import json
import sys, os
from dejavu import Dejavu
from dejavu.logic.recognizer.file_recognizer import FileRecognizer
import pandas as pd
import datetime
from collections import defaultdict

# load config from a JSON file (or anything outputting a python dictionary)
config = {
    "database": {
        "host": "db",
        "user": "postgres",
        "password": "password",
        "database": "dejavu",
    },
    "database_type": "postgres",
}

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


def update_csv(path):
    global vh_dict, vh_example, vh_counts
    csv_path = os.path.join(path, "cm_detection_results.csv")
    out_path = os.path.join(path, "cm_detection_results_matched.csv")
    if not os.path.exists(csv_path):
        print(f"CSV file {csv_path} does not exist")
        return
    foldername = [x for x in os.listdir(path) if "." not in x][0]
    df = pd.read_csv(csv_path)
    df["matched"] = 0
    for index, row in df.iterrows():
        filename = f"{datetime.datetime.strptime(row['start_datetime'], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y%m%d_%H%M%S')}"
        df.loc[index, "matched"] = vh_counts[vh_dict[filename]]
    df.to_csv(out_path, index=False)


vh_dict = {}
vh_example = {}
vh_counts = defaultdict(lambda: 0)
djv = Dejavu(config)


def get_content_set(filename):
    rounded_datetime = datetime.datetime.strptime(filename, "%Y%m%d_%H%M%S").replace(
        second=0, microsecond=0
    )
    global cm_by_time
    return set(
        cm_by_time[rounded_datetime]
        + cm_by_time[rounded_datetime + datetime.timedelta(minutes=1)]
    )


def hash_audiofile(filename):
    global vh_dict, vh_example, vh_counts, djv
    results = djv.recognize(FileRecognizer, filename)["results"]
    stripped_filename = filename.split("/")[-1].split(".")[0]
    # print(filename, results)
    for result in results:
        if result["hashes_matched_in_input"] > 30:
            match_filename = result["song_name"].decode("utf-8")
            vh_dict[stripped_filename] = match_filename
            vh_counts[match_filename] += 1
            # match_set = get_content_set(match_filename)
            # self_set = get_content_set(stripped_filename)
            # if len(match_set & self_set) > 0:
            #     print("matched")
            # else:
            #     print("not matched")
            #     print(match_set, self_set)
            break
    else:
        vh_dict[stripped_filename] = stripped_filename
        vh_counts[stripped_filename] = 1
        djv.fingerprint_file(filename)
    return
    df["matched"] = False
    for idx, loc in df.iterrows():
        vh = format_videohash(loc["videohash"])
        if vh == 0:
            pass
        else:
            if vh in vh_dict:
                vh_counts[vh_dict[vh]] += 1
            else:
                for other in vh_counts:
                    bc = int.bit_count(vh ^ other)
                    if bc < threshold:
                        vh_dict[vh] = other
                        vh_counts[other] += 1
                        vh_example[other].append(
                            loc["start_datetime"].strftime("%Y-%m-%d %H:%M:%S")
                            + " "
                            + loc["channel"]
                            + " "
                            + str(bc)
                        )
                        break
                else:
                    vh_dict[vh] = vh
                    vh_counts[vh] = 1
                    vh_example[vh] = [
                        loc["start_datetime"].strftime("%Y-%m-%d %H:%M:%S")
                        + " "
                        + loc["channel"]
                    ]
    for idx, loc in df.iterrows():
        vh = format_videohash(loc["videohash"])
        if vh != 0:
            df.loc[idx, "matched"] = vh_counts[vh_dict[vh]] > 1

    for k, v in vh_example.items():
        if len(v) > 1 and any(" 5" in x for x in v):
            print(k, v)
    # print(Counter(df["videohash"]).most_common())


def fingerprint_csv(path):
    global vh_dict, vh_example, vh_counts, djv
    csv_path = os.path.join(path, "cm_detection_results.csv")
    if not os.path.exists(csv_path):
        print(f"CSV file {csv_path} does not exist")
        return
    foldername = [x for x in os.listdir(path) if "." not in x][0]
    df = pd.read_csv(csv_path)
    for index, row in df.iterrows():
        file_path = os.path.join(
            path,
            foldername,
            "temp",
            f"{datetime.datetime.strptime(row['start_datetime'], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y%m%d_%H%M%S')}{audio_ext}",
        )
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist")
            continue
        hash_audiofile(file_path)


def process_ground_truth():
    def get_actual_cm():
        # files = Path("../data/FY24_Nov").rglob("*.xlsx")
        # # Filter out files with 1122 and 1129 in the name
        # files = [f for f in files if "1122" in f.name or "1129" in f.name]
        files = ["TX_1001-07テレビCM統計_局別時点リスト_20250131_112705.xlsx"]
        dfs = []

        for f in files:
            print(f)
            df = pd.read_excel(f)
            df["channel"] = "tx"
            dfs.append(df)

        df_raw = pd.concat(dfs)

        # Rearrange columns
        columns = ["channel"] + df_raw.columns[:-1].to_list()
        df_raw = df_raw[columns]
        return df_raw

    df_cm_raw = get_actual_cm()

    def process_cm_data(df_cm_raw):
        # Remove rows from 0 to 13 and make 14 as column header
        df = df_cm_raw.iloc[14:].copy()
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        df.head()

        # Drop columns from 14 till the end except for column named 'channel'
        df = df.iloc[:, :15]

        df = df.fillna("")

        # Reset the index
        df.reset_index(drop=True, inplace=True)
        # Drop rows 0 and 1
        df = df.drop([0, 1])

        # SEQ	番組名	放送開始時刻	放送分数	出稿日付	出稿曜日	出稿\n時刻	TC	CM種類	秒数
        columns = [
            "SEQ",
            "番組名",
            "放送開始時刻",
            "放送分数",
            "出稿日付",
            "出稿曜日",
            "出稿時刻",
            "TC",
            "CM種類",
            "秒数",
        ]
        columns_en = [
            "seq",
            "program",
            "pr_start_time",
            "pr_duration",
            "date",
            "day_of_week",
            "cm_time",
            "tc",
            "cm_type",
            "seconds",
        ]
        # 広告主名	銘柄名	CM内容	前コメント
        sub_columns = ["広告主名", "銘柄名", "CM内容", "前コメント"]
        sub_columns_en = ["advertiser", "brand", "content", "comment"]
        df.columns = ["channel"] + columns_en + sub_columns_en

        # Drop rows where 'seconds' contain '' or '秒数'
        df = df[~df["seconds"].isin(["", "秒数"])]

        # Convert 'seconds' to int
        df["seconds"] = df["seconds"].astype(int)

        # Convert seconds to int
        return df

    df_cm = process_cm_data(df_cm_raw)

    def fix_cm_time(df_cm):

        def convert_time(x):
            if pd.isna(x) or x == "":
                return x
            try:
                hours = int(x[:2])
                minutes = x[3:]
                return f"{hours % 24:02d}:{minutes}"
            except (ValueError, IndexError):
                return x

        df_cm["cm_time"] = df_cm["cm_time"].apply(convert_time)
        return df_cm

    df_cm = fix_cm_time(df_cm)
    df_cm["date"] = df_cm["date"].str.strip()
    df_cm["cm_time"] = df_cm["cm_time"].str.strip()

    # Create datetime only for rows where both date and cm_time are not empty
    mask = (
        (df_cm["date"].notna())
        & (df_cm["date"] != "")
        & (df_cm["cm_time"].notna())
        & (df_cm["cm_time"] != "")
    )
    df_cm.loc[mask, "start_datetime"] = pd.to_datetime(
        df_cm.loc[mask, "date"] + " " + df_cm.loc[mask, "cm_time"],
        format="%Y/%m/%d %H:%M",
        errors="coerce",
    )
    return df_cm


if __name__ == "__main__":
    df_cm = process_ground_truth()
    cm_by_time = defaultdict(list)
    for idx, row in df_cm.iterrows():
        cm_by_time[row["start_datetime"]].append(row["content"])
    recursive_process_directory(sys.argv[1], 5, fingerprint_csv)
    recursive_process_directory(sys.argv[1], 5, update_csv)
