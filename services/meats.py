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

meat_producer = Producer(producer_config)
cheese_consumer = Consumer(consumer_config)

#####################################################


MEATS = [
    "pepperoni",
    "sausage",
    "ham",
    "anchovies",
    "salami",
    "bacon",
    "pepperoni",
    "sausage",
    "ham",
    "anchovies",
    "salami",
    "bacon",
]


def service_add_meats(order_id: str, pizza: dict) -> None:
    pizza["meats"] = MEATS[random.randint(0, len(MEATS) - 1)]
    meat_producer.produce(
        "pizza-with-meats",
        key=order_id,
        value=json.dumps(obj=pizza),
    )


######################################################################


def consume_cheese() -> None:
    cheese_consumer.subscribe(["pizza-with-cheese"])
    while True:
        if msg := cheese_consumer.poll(0.1):
            pizza: dict[Any, str] = json.loads(msg.value())
            service_add_meats(msg.key(), pizza)


######################################################################

if __name__ == "__main__":
    consume_cheese()
