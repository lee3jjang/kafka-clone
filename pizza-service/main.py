import json
from typing import Awaitable, Callable, Literal, Self
import uuid
from dataclasses import dataclass, asdict, field

from confluent_kafka import Producer, Consumer  # type: ignore[import-untyped]
from configparser import ConfigParser
from fastapi import FastAPI, Request, Response

######################################################################


@dataclass
class Pizza:
    order_id: str = ""
    sauce: str = ""
    cheese: str = ""
    meats: str = ""
    veggies: str = ""

    @classmethod
    def from_dict(
        cls,
        data: dict[Literal["order_id", "sauce", "cheese", "meats", "veggies"], str],
    ) -> Self:
        return cls(
            order_id=data["order_id"],
            sauce=data["sauce"],
            cheese=data["cheese"],
            meats=data["meats"],
            veggies=data["veggies"],
        )


@dataclass
class PizzaOrder:
    count: int
    id: str = field(default_factory=lambda: str(uuid.uuid4().int))
    _pizzas: list[Pizza] = field(default_factory=list)

    def add_pizza(self, pizza: Pizza) -> None:
        self._pizzas.append(pizza)

    def get_pizzas(self) -> list[Pizza]:
        return self._pizzas


######################################################################

config_parser = ConfigParser(interpolation=None)
config_file = open("config.properties", "r")
config_parser.read_file(config_file)
producer_config = dict(config_parser["kafka_client"])
consumer_config = dict(config_parser["kafka_client"])
consumer_config.update(config_parser["consumer"])

pizza_producer = Producer(producer_config)
pizza_consumer = Consumer(consumer_config)

######################################################################

pizza_warmer: dict[str, PizzaOrder] = {}


def service_order_pizzas(count: int) -> str:
    order = PizzaOrder(count)
    pizza_warmer[order.id] = order

    for _ in range(count):
        new_pizza = Pizza()
        new_pizza.order_id = order.id
        pizza_producer.produce(
            "pizza",
            key=order.id,
            value=json.dumps(asdict(new_pizza)),
        )
    pizza_producer.flush()

    return order.id


def service_add_pizza(pizza: Pizza) -> None:
    if (order_id := pizza.order_id) in pizza_warmer.keys():
        order = pizza_warmer[order_id]
        order.add_pizza(pizza)


def service_load_orders() -> None:
    pizza_consumer.subscribe(["pizza-with-veggies"])
    while True:
        if event := pizza_consumer.poll(1.0):
            pizza: Pizza = Pizza.from_dict(json.loads(event.value()))
            service_add_pizza(pizza)


######################################################################

app = FastAPI()


@app.middleware("http")
async def sample_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    response = await call_next(request)
    return response


@app.get("/order/{count}")
def order_pizzas(count: int):
    order_id = service_order_pizzas(int(count))
    return {"order_id": order_id}


######################################################################
