from influxdb import InfluxDBClient
from datetime import datetime
import re
import csv

# InfluxDB 1.8 connection settings
INFLUX_HOST = "192.168.1.216"
INFLUX_PORT = 8086
INFLUX_DB = "flink_metrics"

LOG_DIR = "logs/"

METRICS = {
    "backpressure": "taskmanager_job_task_backPressuredTimeMsPerSecond",
    "throughput": "taskmanager_job_task_numRecordsOutPerSecond",
    "input_rate": "taskmanager_job_task_numRecordsInPerSecond",
    "jvm_cpu_load": "taskmanager_Status_JVM_CPU_Load",
    "jvm_cpu_time": "taskmanager_Status_JVM_CPU_Time",
    "jvm_gc_time": "taskmanager_Status_JVM_GarbageCollector_All_Time",
    "jvm_heap_used": "taskmanager_Status_JVM_Memory_Heap_Used",
    "jvm_threads": "taskmanager_Status_JVM_Threads_Count",
    "shuffle_netty_used_segments": "taskmanager_Status_Shuffle_Netty_UsedMemorySegments",
    "remote_bytes_per_sec": "taskmanager_job_task_Shuffle_Netty_Input_numBytesInRemote",
}

METRIC_TO_FIELD = {
    "backpressure": "mean",
    "throughput": "rate",
    "input_rate": "rate",
    "jvm_cpu_time": "value",
    "jvm_gc_time": "value",
    "jvm_heap_used": "value",
    "jvm_threads": "value",
    "shuffle_netty_used_segments": "value",
    "remote_bytes_per_sec": "count"
}

AGG_METRICS_MEAN = {
    "backpressure"
}

AGG_METRICS_SUM = {
    "jvm_heap_used",
    "jvm_gc_time",
    "jvm_cpu_time",
    "jvm_threads",
    "shuffle_netty_used_segments",
    "remote_bytes_per_sec"
}


def parse_log_file(log_path):
    with open(log_path, "r") as f:
        lines = f.readlines()

    entries = []
    for i in range(0, len(lines), 2):
        start_line = lines[i].strip()
        end_line = lines[i + 1].strip() if i + 1 < len(lines) else None

        start_match = re.match(r"(.*?) - Starting config: (.*)", start_line)
        end_match = re.match(r"(.*?) - Finished config: (.*)", end_line) if end_line else None

        if start_match and end_match:
            entries.append({
                "label": start_match.group(2),
                "start": start_match.group(1),
                "end": end_match.group(1)
            })
    return entries

def query_mean_metric(client, measurement, start, end):
    query = f"""
    SELECT MEAN("value") FROM "{measurement}"
    WHERE time >= '{start}' AND time <= '{end}'
    """
    # print(query)
    result = client.query(query)
    points = list(result.get_points())
    return points[0]["mean"] if points else None

def query_value_sum(client, measurement, start, end):
    query = f"""
    SELECT SUM("value") FROM "{measurement}"
    WHERE time >= '{start}' AND time <= '{end}'
    """
    # print(query)
    result = client.query(query)
    points = list(result.get_points())
    return points[0]["mean"] if points else None

def query_count_sum(client,measurement, start, end):
    query = f"""
    SELECT SUM("count") FROM "{measurement}"
    WHERE time >= '{start}' AND time <= '{end}'
    GROUP BY time(1s)
    """
    # print(query)
    result = client.query(query)
    points = list(result.get_points())
    # print(points)

    # Extract and average non-None "sum" values
    values = [p["sum"] for p in points if p.get("sum") is not None]
    avg = sum(values) / len(values) if values else 0

    return avg

def query_rate_sum(client,measurement, start, end):
    query = f"""
    SELECT SUM("rate") FROM "{measurement}"
    WHERE time >= '{start}' AND time <= '{end}'
    GROUP BY time(1s)
    """
    # print(query)
    result = client.query(query)
    points = list(result.get_points())
    # print(points)

    # Extract and average non-None "sum" values
    values = [p["sum"] for p in points if p.get("sum") is not None]
    avg = sum(values) / len(values) if values else 0

    return avg

def query_value_sum(client, measurement, start, end):
    query = f"""
    SELECT SUM("value") FROM "{measurement}"
    WHERE time >= '{start}' AND time <= '{end}'
    GROUP BY time(1s)
    """
    # print(query)
    result = client.query(query)
    points = list(result.get_points())
    # print(points)

    # Extract and average non-None "sum" values
    values = [p["sum"] for p in points if p.get("sum") is not None]
    avg = sum(values) / len(values) if values else 0

    return avg

def parse_config_label(label):
    """
    Parses config name like 'tm8x1-40k' into structured fields.
    Returns: dict with keys 'num_tms', 'slots_per_tm', 'input_rate'
    """
    match = re.match(r"tm(\d+)x(\d+)-(\d+)k", label)
    if match:
        return {
            "num_tms": int(match.group(1)),
            "slots_per_tm": int(match.group(2)),
            "input": int(match.group(3)) * 1000
        }
    else:
        return {
            "num_tms": None,
            "slots_per_tm": None,
            "input": None
        }


BENCHMARKS= {
    "StateMachine": "local:///opt/flink/examples/streaming/StateMachineExample.jar",
    "WindowJoin": "local:///opt/flink/examples/streaming/WindowJoin.jar",
    # "WordCount": "local:///opt/flink/examples/streaming/WordCount.jar",
    # "SessionWindow": "local:///opt/flink/examples/streaming/SessionWindowing.jar",
}

RPS = {
    "StateMachine": [10000, 50000, 100000, 200000, 400000],
    "WindowJoin": [1, 3, 5, 7, 9, 11, 19, 25, 50, 100, 200, 500, 1000],
    # "WordCount": [10000, 50000, 100000, 200000, 400000],
    # "SessionWindow": [10000, 50000, 100000, 200000, 400000],
}

CONFIGS = [
    {
        "label": "tm8x1",
        "task_slots": 1,
        "cpu": 1,
        "memory": "2048m",
        "replicas": 8,
        
    },
    {
        "label": "tm2x4",
        "task_slots": 4,
        "cpu": 4,
        "memory": "8192m",
        "replicas": 2
    }
]

def main():

    for benchmark in BENCHMARKS.keys():
        print(f"Processing benchmark: {benchmark}")
        for rps in RPS[benchmark]:
            print(f"  RPS: {rps}")
            for config in CONFIGS:
                print(f"    Config: {config['label']}")
                config_label = config["label"] 

                log_path = LOG_DIR + benchmark + "_" + config_label + "_" + str(rps) + "_log.txt"

                entries = parse_log_file(log_path)
                output_csv = LOG_DIR + benchmark + "_" + config_label + "_" + str(rps) + "_results.csv"

                client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DB)
                # Prepare header
                metric_names = list(METRICS.keys())
                fieldnames = ["config", "num_tms", "slots_per_tm", "input"] + metric_names + ["time_start", "time_end"]
                
                with open(output_csv, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                    for entry in entries:
                        label = entry["label"]
                        start = entry["start"]
                        end = entry["end"]

                        print(f"\n🔍 Config: {label}")
                        row = {
                            "config": label,
                            "time_start": start,
                            "time_end": end
                        }

                        # Parse and add structured config values
                        row.update(parse_config_label(label))

                        for metric_label, measurement in METRICS.items():
                            
                            # avg_value = query_avg_metric(client, measurement, start, end)
                            # elif metric_label == "throughput" or metric_label == "input_rate":
                            if metric_label in ["throughput", "input_rate"]:
                                avg_value = query_rate_sum(client, measurement, start, end)
                            elif metric_label == "remote_bytes_per_sec":
                                avg_value = query_count_sum(client, measurement, start, end)
                            elif metric_label in AGG_METRICS_MEAN:
                                avg_value = query_mean_metric(client, measurement, start, end)
                            else:
                                avg_value = query_value_sum(client, measurement, start, end)
                            if avg_value is not None:
                                print(f"  {metric_label.capitalize()}: {avg_value:.2f}")
                                row[metric_label] = avg_value
                            else:
                                print(f"  {metric_label.capitalize()}: No data")
                                row[metric_label] = None
                        writer.writerow(row)

if __name__ == "__main__":
    main()
