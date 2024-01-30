import os
import json
import requests
import logging
from kafka import KafkaConsumer

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.FileHandler("consumer.log"), logging.StreamHandler()])

class ElasticSearchKafkaUploadRecord:
    def __init__(self, json_data, hash_key, index_name):
        self.hash_key = hash_key
        self.json_data = json_data
        self.index_name = index_name.lower()

    def upload(self):
        """
        Uploads records on Elastic Search Cluster
        :return
        """
        URL = "http://{}/{}/_doc/{}".format(os.getenv("SERVER_END_POINT"), self.index_name, self.hash_key)
        headers = {"Content-Type": "application/json"}
        response = requests.request("PUT", URL, headers=headers, data=json.dumps(self.json_data))
        
        logging.info(f"Upload response: {response}")
        return {"status": 200, "data": {"message": "record uploaded to Elastic Search"}}

def main():
    # Now these will use the values from the environment variables set in .bashrc
    consumer = KafkaConsumer(os.getenv("KAFKA_TOPIC"),
                             group_id='my-consumer-group', 
                             bootstrap_servers=os.getenv("KAFKA_SERVER"),
                             auto_offset_reset='earliest',
                             enable_auto_commit=True
                             )

    for msg in consumer:
        payload = json.loads(msg.value)
        payload["meta_data"] = {
            "topic": msg.topic,
            "partition": msg.partition,
            "offset": msg.offset,
            "timestamp": msg.timestamp,
            "timestamp_type": msg.timestamp_type,
            "key": msg.key,
        }

        logging.info(f"Consumed record with offset: {msg.offset}")

        helper = ElasticSearchKafkaUploadRecord(json_data=payload,
                                                index_name=payload.get("meta_data").get("topic"),
                                                hash_key=payload.get("meta_data").get("offset"))
        response = helper.upload()

if __name__ == "__main__":
    main()
