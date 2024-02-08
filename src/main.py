import os
import sys
import time
from datetime import datetime
import paho.mqtt.client as mqtt
import yaml

QOS = 1
EVENT_TYPE = "__KRKN_HEATBEAT__"
DFLT_CONFIG_PATH = "config.yml"
DFLT_CLIENT_ID = "kraken-edge-heartbeater"
DFLT_TOPIC = "krakeniot"

def load_yaml(path):
    with open(path, 'r') as file:
        docs = yaml.safe_load(file)
    return docs

def main():
    print("Starting `Kraken Edge HeartBeater`...")
    
    # Load config
    config_path = sys.argv[1] if len(sys.argv) > 1 else DFLT_CONFIG_PATH
    docs = load_yaml(config_path)
    config = docs[0]
    
    # Display the details of config 
    print(f"Loaded configs from {config_path}.")
    print(f"MQTT Host: {config['mqtt']['host']}")
    print(f"MQTT Topic: {config['mqtt']['topic']}")
    print(f"Client ID: {config['client']['id']}")
    print(f"Publish Interval: {config['client']['interval']}")
    
    # Set configs
    host = config['mqtt']['host']
    topic = config['mqtt']['topic'] or DFLT_TOPIC
    client_id = config['client']['id'] or DFLT_CLIENT_ID
    interval = config['client']['interval']
    
    # Define the set of options to create the client
    dt = datetime.now()
    mqtt_client_id = f"{client_id}_{int(dt.timestamp())}"
    create_opts = {
        'server_uri': host,
        'client_id': mqtt_client_id,
    }
    client = mqtt.Client(client_id)
    client.connect(host)
    
    # Define the set of options for the connection
    conn_opts = {
        'keep_alive': 20,
        'clean_session': True,
    }
    
    # Connect and wait for it to complete or fail
    try:
        client.connect(host)
        print("Connected to MQTT broker successfully.")
        print("Start to publish heartbeats...")
        
        # Publish heartbeat to the topic of Kraken IoT Collector
        while True:
            dt = datetime.now()
            timestamp = int(dt.timestamp())
            content = f'{{"eventType": "{EVENT_TYPE}", "id": "{client_id}", "dt": {timestamp}}}'
            print(f"Publishing messages on the {topic} topic")
            client.publish(topic, content, QOS)
            time.sleep(interval)
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        client.disconnect()
        print("Disconnected from the broker")

if __name__ == "__main__":
    main()

