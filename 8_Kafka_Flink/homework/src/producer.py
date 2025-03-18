import csv
import json
from time import time
from kafka import KafkaProducer

from Trips import Trip


def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic_name = "green-trips"
    csv_file = 'green_tripdata_2019-10.csv' 

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            message = Trip(d=row).to_dict()
            producer.send(topic=topic_name, value=message)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    t0 = time()
    main()
    t1 = time()
    print(f'took {(t1 - t0):.2f} seconds')