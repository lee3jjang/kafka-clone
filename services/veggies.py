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

veggie_producer = Producer(producer_config)
meat_consumer = Consumer(consumer_config)

#####################################################


VEGGIES = [
    "tomato",
    "olives",
    "onions",
    "peppers",
    "pineapple",
    "mushrooms",
    "tomato",
    "olives",
    "onions",
    "peppers",
    "pineapple",
    "mushrooms",
]


def service_add_meats(order_id: str, pizza: dict) -> None:
    pizza["meats"] = VEGGIES[random.randint(0, len(VEGGIES) - 1)]
    veggie_producer.produce(
        "pizza-with-veggies",
        key=order_id,
        value=json.dumps(obj=pizza),
    )


######################################################################


def consume_meats() -> None:
    meat_consumer.subscribe(["pizza-with-meats"])
    while True:
        if msg := meat_consumer.poll(0.1):
            pizza: dict[Any, str] = json.loads(msg.value())
            service_add_meats(msg.key(), pizza)


######################################################################

if __name__ == "__main__":
    consume_meats()
