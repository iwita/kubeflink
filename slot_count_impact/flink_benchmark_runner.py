import subprocess
import yaml
import time
from pathlib import Path
from datetime import datetime, timezone
import os


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

BENCHMARKS= {
    # "StateMachine": "local:///opt/flink/examples/streaming/StateMachineExample.jar",
    "WindowJoin": "local:///opt/flink/examples/streaming/WindowJoin.jar",
    # "WordCount": "local:///opt/flink/examples/streaming/WordCount.jar",
    # "SessionWindow": "local:///opt/flink/examples/streaming/SessionWindowing.jar",
}

RPS = {
    "StateMachine": [10000, 50000, 100000, 200000, 400000],
    # "WindowJoin": [10, 20, 50, 100, 500],
    "WindowJoin": [5000, 10000],
    "WordCount": [10000, 50000, 100000, 200000, 400000],
    "SessionWindow": [10000, 50000, 100000, 200000, 400000],
}

input_var = {
    "StateMachine": "rps",
    "WindowJoin": "rate",
}

# Path to your base YAML file
YAML_PATH = "../example/basic.yaml"

# List of benchmarking configurations
CONFIG_LABELS = ["tm8x1", "tm2x4"]  # Add more labels if needed

# Name of the FlinkDeployment resource
FLINK_DEPLOYMENT_NAME = "basic-example"

def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)

def update_config(yaml_data, config, benchmark, rps):
    conf = yaml_data["spec"]["flinkConfiguration"]
    conf["metrics.reporter.influxdb.tags"] = f"config={config['label']}"
    conf["taskmanager.numberOfTaskSlots"] = str(config["task_slots"])
    
    yaml_data["spec"]['job']["jarURI"] = BENCHMARKS[benchmark]
    yaml_data["spec"]["job"]["args"] = [
        "--"+input_var[benchmark],
        str(rps)
    ]

    tm_spec = yaml_data["spec"]["taskManager"]["resource"]
    tm_spec["cpu"] = config["cpu"]
    tm_spec["memory"] = config["memory"]


def save_yaml(data, path):
    with open(path, "w") as f:
        yaml.safe_dump(data, f)


def apply_deployment(yaml_file):
    subprocess.run(["kubectl", "apply", "-f", yaml_file], check=True)

def delete_deployment(yaml_file):
    subprocess.run(["kubectl", "delete", "-f", yaml_file], check=True)

def are_taskmanagers_ready(label_selector="component=taskmanager,app=basic-example", expected_replicas=4):
    try:
        output = subprocess.check_output(
            ["kubectl", "get", "pods", "-l", label_selector, "--no-headers"],
            text=True
        )
        lines = output.strip().split("\n")
        if len(lines) < expected_replicas:
            return False  # not all replicas started yet

        for line in lines:
            columns = line.split()
            status = columns[2]
            ready = columns[1]
            if status != "Running" or not ready.startswith("1/1"):
                return False
        return True

    except subprocess.CalledProcessError as e:
        print("Failed to get pods:", e)
        return False

def wait_for_ready(name, replicas, timeout=300):
    print("⏳ Waiting for FlinkDeployment to be READY...")
    for _ in range(timeout // 5):
        if are_taskmanagers_ready("component=taskmanager,app=basic-example", expected_replicas=replicas):
            return True
        time.sleep(5)
    print("⚠️ Timeout waiting for FlinkDeployment to be READY.")
    return False

def run_benchmark(config, base_yaml_path, benchmark, rps):
    print(f"\n🚀 Starting config: {config['label']}")
    yaml_data = load_yaml(base_yaml_path)

    update_config(yaml_data, config, benchmark, rps)

    modified_yaml_path = f"/tmp/flink_benchmark_{config['label']}.yaml"
    save_yaml(yaml_data, modified_yaml_path)

    apply_deployment(modified_yaml_path)

    start = -1
    if wait_for_ready(FLINK_DEPLOYMENT_NAME, config["replicas"]):
        print("⏲️ Waiting 5 minutes for stabilization.")
        time.sleep(10 * 60)
        print("⏲️ Record for 5 minutes")
        start = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        time.sleep(10 * 60)

    else:
        print("⛔ Skipping wait due to deployment not becoming READY.")

    print(f"🧹 Cleaning up config: {config['label']}")
    delete_deployment(modified_yaml_path)
    end = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return start, end


def main():
    
    for benchmark in BENCHMARKS:
        for config in CONFIGS:
            for rps in RPS[benchmark]:
                # create logs dir of not exists
                log_file = f"logs/{benchmark}_{config['label']}_{rps}_log.txt"
                os.makedirs(os.path.dirname(log_file), exist_ok=True)
                start, end = run_benchmark(config, YAML_PATH, benchmark, rps)

                log_entry = f"{start} - Starting config: {config['label']}\n"
                log_entry += f"{end} - Finished config: {config['label']}\n"
                print(log_entry.strip())

                # Append to log file
                with log_file.open("a") as f:
                    f.write(log_entry)

main()
