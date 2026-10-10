# Lección 02 - Consultas avanzadas y a qué shards llega cada una.
from lab.client import get_db
from lab.cluster import route
from lab.show import step, table, title

db = get_db()
orders = db.orders

title("02 - Consultas y enrutamiento")

customer = orders.find_one({"country": "Chile"}, {"customer_id": 1})["customer_id"]
queries = [
    ("Un cliente (shard key completa)", {"country": "Chile", "customer_id": customer}),
    ("Un país (prefijo de la shard key)", {"country": "Perú"}),
    ("Varios países ($in)", {"country": {"$in": ["Argentina", "Brasil"]}}),
    ("Rango de países ($gte/$lt)", {"country": {"$gte": "A", "$lt": "C"}}),
    ("Estado (no es parte de la shard key)", {"status": "cancelled"}),
    ("Ciudad + total > 1000", {"city": "Lima", "total": {"$gt": 1000}}),
    ("Solo customer_id (sin el prefijo country)", {"customer_id": customer}),
]

step("Resultados y shards consultados por cada filtro")
table([{"consulta": label, "resultados": orders.count_documents(query)} | route(db, query)
       for label, query in queries])
print("SINGLE_SHARD = consulta dirigida (rápida, escala). SHARD_MERGE = scatter-gather: pregunta a todos y une.")
print("Con shard key {country, customer_id}, filtrar solo por customer_id NO sirve: falta el prefijo country.")

step("Operadores avanzados: $elemMatch (un mismo ítem cumple ambas condiciones)")
query = {"country": "Colombia", "items": {"$elemMatch": {"category": "Electrónica", "quantity": {"$gte": 3}}}}
print(orders.count_documents(query), "órdenes de Colombia con 3+ unidades de un producto de Electrónica")

step("$expr: órdenes con un solo ítem cuyo total supera 1500")
query = {"$expr": {"$and": [{"$eq": [{"$size": "$items"}, 1]}, {"$gt": ["$total", 1500]}]}}
print(orders.count_documents(query), "órdenes")

step("Orden y límite: las 5 órdenes más caras de México (mongos une y ordena lo que devuelve cada shard)")
table(orders.find({"country": "México"}, {"_id": 0, "order_number": 1, "city": 1, "total": 1})
      .sort("total", -1).limit(5))

step("distinct: países y estados")
print(orders.distinct("country"))
print(orders.distinct("status"))
