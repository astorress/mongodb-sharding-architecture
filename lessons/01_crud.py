# Lección 01 - CRUD a través de mongos.
#
# Para la aplicación, mongos se ve como una base de datos normal: el código es el mismo de siempre.
# La diferencia está en el enrutamiento: si la operación incluye la shard key (country + customer_id),
# mongos la envía a un solo shard; si no, tiene que preguntarle a todos.
from datetime import datetime, timezone

from lab.client import get_db
from lab.cluster import route
from lab.show import show, step, table, title

db = get_db()
orders = db.orders

title("01 - CRUD a través de mongos")

orders.delete_many({"order_number": {"$gte": 900000}})

step("Create: insert_one / insert_many (mongos lee la shard key de cada documento para ubicarlo)")
order = {"order_number": 900001, "customer_id": "C99999", "country": "Colombia", "city": "Bogotá",
         "status": "pending", "items": [{"product": "Libro", "category": "Libros", "quantity": 2, "price": 15.0}],
         "total": 30.0, "created_at": datetime.now(timezone.utc)}
print("inserted_id:", orders.insert_one(order).inserted_id)
orders.insert_many([
    {"order_number": 900002, "customer_id": "C99999", "country": "Colombia", "city": "Bogotá", "status": "paid",
     "items": [], "total": 120.0, "created_at": datetime.now(timezone.utc)},
    {"order_number": 900003, "customer_id": "C99998", "country": "España", "city": "Madrid", "status": "paid",
     "items": [], "total": 75.5, "created_at": datetime.now(timezone.utc)},
])

step("Read: find con la shard key completa -> un solo shard")
query = {"country": "Colombia", "customer_id": "C99999"}
show(orders.find(query, {"_id": 0, "order_number": 1, "status": 1, "total": 1}))
table([route(db, query)])

step("Read: find sin la shard key -> todos los shards")
query = {"order_number": 900003}
show(orders.find_one(query, {"_id": 0, "order_number": 1, "country": 1}))
table([route(db, query)])

step("Update: update_one con la shard key en el filtro")
result = orders.update_one({"country": "Colombia", "customer_id": "C99999", "order_number": 900001},
                           {"$set": {"status": "paid"}, "$push": {"items": {"product": "Balón", "quantity": 1}}})
print("matched:", result.matched_count, "modified:", result.modified_count)

step("Update: update_many sin shard key (mongos lo envía a todos los shards)")
result = orders.update_many({"order_number": {"$gte": 900000}}, {"$inc": {"total": 1}})
print("modified:", result.modified_count)

step("Update: cambiar el valor de la shard key mueve el documento de rango (y quizá de shard)")
result = orders.update_one({"country": "España", "customer_id": "C99998", "order_number": 900003},
                           {"$set": {"country": "México"}})
print("modified:", result.modified_count, "-> ahora vive en el rango de México")

step("Delete: delete_one y delete_many")
print("delete_one:", orders.delete_one({"country": "Colombia", "customer_id": "C99999",
                                        "order_number": 900002}).deleted_count)
print("delete_many:", orders.delete_many({"order_number": {"$gte": 900000}}).deleted_count)
