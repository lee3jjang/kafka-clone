import json
import random
from typing import Any

from confluent_kafka import Producer, Consumer  # type: ignore[import-untyped]
from configparser import ConfigParser


#####################################################

config_parser = ConfigParser(interpolation=None)
config_file = open("config.properties", "r")
config_parser.read_file(config_file)
producer_config = dict(config_parser["kafka_client"])
consumer_config = dict(config_parser["kafka_client"])
consumer_config.update(config_parser["consumer"])

cheese_producer = Producer(producer_config)
sauce_consumer = Consumer(consumer_config)

#####################################################

CHEESES = [
    "extra",
    "none",
    "three cheese",
    "goat cheese",
    "extra",
    "three cheese",
    "goat cheese",
]


def service_add_cheese(order_id: str, pizza: dict) -> None:
    pizza["cheese"] = CHEESES[random.randint(0, len(CHEESES) - 1)]
    cheese_producer.produce(
        "pizza-with-cheese",
        key=order_id,
        value=json.dumps(pizza),
    )


######################################################################


def consume_sauce() -> None:
    sauce_consumer.subscribe(["sauce"])
    while True:
        if msg := sauce_consumer.poll(0.1):
            pizza: dict[Any, str] = json.loads(msg.value())
            service_add_cheese(msg.key(), pizza)


######################################################################

if __name__ == "__main__":
    consume_sauce()
