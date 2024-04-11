#!/usr/bin/env python
import json
import time as tm
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from datetime import datetime
from random import uniform

from confluent_kafka import Producer

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)


    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


    def generate_random_data():
        return {
            'service_id': "imagenet_image_classification",
            'client_id': "raspi-1",
            'prediction': uniform(0.5, 1.0),  # Random float between 0.8 and 1.0
            'compute_time': uniform(5, 15),  # Random integer between 5 and 15
            'pred_accuracy': uniform(0.5, 0.7),  # Random float between 0.5 and 0.7
            'total_qoe': uniform(0.7, 0.9),  # Random float between 0.7 and 0.9
            'accuracy_qoe': uniform(0.3, 0.5),  # Random float between 0.3 and 0.5
            'delay_qoe': uniform(0.3, 0.5),  # Random float between 0.3 and 0.5
            'req_acc': uniform(0.7, 0.9),  # Random float between 0.7 and 0.9
            'req_delay': uniform(0.2, 0.4),  # Random float between 0.2 and 0.4
            'model': 'SqueezeNet',
            'added_time': datetime.now().strftime("%d-%m-%Y %I:%M:%S")
        }


    topic = "raw-accuracy"
    start_time = tm.time()
    while tm.time() - start_time < 200:  # Loop for 100 seconds
        data = generate_random_data()
        producer.produce(topic, json.dumps(data), "EDGE-1", callback=delivery_callback)
        producer.poll(0)  # Serve delivery callback
        tm.sleep(0.5)  # Adjust sleep time as needed to simulate production rate

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
