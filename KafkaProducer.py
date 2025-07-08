from confluent_kafka import Producer
import json
import time
from dotenv import dotenv_values

class KafkaProducer:
    def __init__(self):
        self.application_config = dotenv_values(".env")
        self.kafka_required_confs = {
            "bootstrap.servers" : self.application_config["bootstrap.servers"],
            "security.protocol" : self.application_config["security.protocol"],
            "sasl.mechanism" : self.application_config["sasl.mechanism"],
            "sasl.username" : self.application_config["sasl.username"],
            "sasl.password" : self.application_config["sasl.password"],
            "client.id" : self.application_config["client.id"]
        }

    def delivery_callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            key = msg.key().decode('utf-8')
            invoice_id = json.loads(msg.value().decode('utf-8'))["InvoiceNumber"]
            print(f"Produced event : key = {key} value = {invoice_id}")

    def produce_invoices(self, producer):
        with open("data/invoices.json") as lines:
            for line in lines:
                invoice = json.loads(line)
                store_id = invoice["StoreID"]
                producer.produce(self.application_config["topic"], key=store_id, value=json.dumps(invoice), callback=self.delivery_callback)
                time.sleep(0.5)
                producer.poll(1)

    def start(self):
        kafka_producer = Producer(self.kafka_required_confs)
        self.produce_invoices(kafka_producer)
        kafka_producer.flush(2)

if __name__ == "__main__":
    invoice_producer = KafkaProducer()
    invoice_producer.start()