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

sauce_producer = Producer(producer_config)
pizza_consumer = Consumer(consumer_config)

#####################################################


SAUCES = [
    "regular",
    "light",
    "extra",
    "none",
    "alfredo",
    "regular",
    "light",
    "extra",
    "alfredo",
]


def service_add_sauce(order_id: str, pizza: dict) -> None:
    pizza["sauce"] = SAUCES[random.randint(0, len(SAUCES) - 1)]
    sauce_producer.produce(
        "pizza-with-sauce",
        key=order_id,
        value=json.dumps(obj=pizza),
    )


######################################################################


def consume_pizza() -> None:
    pizza_consumer.subscribe(["pizza"])
    while True:
        if msg := pizza_consumer.poll(0.1):
            pizza: dict[Any, str] = json.loads(msg.value())
            service_add_sauce(msg.key(), pizza)


######################################################################

if __name__ == "__main__":
    consume_pizza()
