import time
from datetime import datetime
import paho.mqtt.client as mqtt
import yaml

QOS = 1

# ブローカー側で設定があるため変更不可
EVENT_TYPE = "__KRKN_HEATBEAT__"

# その他
DFLT_CONFIG_PATH = "../config.yaml"
DFLT_CLIENT_ID = "kraken-edge-heartbeater-py"
DFLT_TOPIC = "krakeniot"


def load_yaml(path):
    with open(path, "r") as file:
        docs = yaml.safe_load(file)
    return docs


def main():
    print("Starting `Kraken Edge HeartBeater`...")

    # Load config
    config_path = DFLT_CONFIG_PATH
    config = load_yaml(config_path)

    # Display the details of config
    print(f"Loaded configs from {config_path}.")
    print(f"MQTT IP: {config['mqtt']['host']}")
    print(f"MQTT port: {config['mqtt']['port']}")
    print(f"MQTT Topic: {config['mqtt']['topic']}")
    print(f"Client ID: {config['client']['id']}")
    print(f"Publish Interval: {config['client']['interval']}")

    # Set configs
    host = config["mqtt"]["host"]
    port = config["mqtt"]["port"]
    topic = config["mqtt"]["topic"]
    client_id = config["client"]["id"]
    interval = config["client"]["interval"]

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)

    # Connect and wait for it to complete or fail
    try:
        #rustと違いIP,portで指定する必要あり
        client.connect(host, port, 60)
        client.loop()
        print("Connected to MQTT broker successfully.")
        print("Start to publish heartbeats...")

        # Publish heartbeat to the topic of Kraken IoT Collector
        while True:
            dt = datetime.now()
            timestamp = int(dt.timestamp())
            content = f'{{"eventType": "{EVENT_TYPE}", "id": "{client_id}", "dt": {timestamp}}}'
            print(f"Publishing messages on the {topic} topic")
            client.publish(topic=topic, payload=content, qos=QOS)
            time.sleep(interval)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        client.disconnect()
        print("Disconnected from the broker")


if __name__ == "__main__":
    main()