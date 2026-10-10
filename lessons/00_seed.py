# Lección 00 - Crear la colección fragmentada `orders` y cargar los datos.
#
#   1. enableSharding: la base `shop` se crea con shard1rs como "primary shard".
#   2. shardCollection: `orders` se reparte entre shards según la shard key {country: 1, customer_id: 1}.
#   3. insert_many: la aplicación escribe a mongos; mongos decide a qué shard va cada documento.
import random
import time
from datetime import datetime, timedelta, timezone

from lab.client import get_client
from lab.cluster import distribution
from lab.config import MONGO_DB
from lab.show import step, table, title

COUNTRIES = {
    "Argentina": ["Buenos Aires", "Córdoba", "Rosario"],
    "Brasil": ["São Paulo", "Río de Janeiro", "Brasilia"],
    "Chile": ["Santiago", "Valparaíso", "Concepción"],
    "Colombia": ["Bogotá", "Medellín", "Cali"],
    "Ecuador": ["Quito", "Guayaquil", "Cuenca"],
    "España": ["Madrid", "Barcelona", "Valencia"],
    "México": ["Ciudad de México", "Guadalajara", "Monterrey"],
    "Perú": ["Lima", "Arequipa", "Cusco"],
}
PRODUCTS = [("Portátil", "Electrónica", 900), ("Celular", "Electrónica", 450), ("Audífonos", "Electrónica", 80),
            ("Silla", "Hogar", 120), ("Lámpara", "Hogar", 35), ("Cafetera", "Hogar", 60),
            ("Camiseta", "Moda", 20), ("Zapatos", "Moda", 70), ("Libro", "Libros", 15), ("Balón", "Deportes", 25)]
STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled"]
NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)


def orders(count=20000, customers=2000):
    rng = random.Random(42)
    people = [(f"C{i:05d}", rng.choice(list(COUNTRIES))) for i in range(1, customers + 1)]
    docs = []
    for number in range(1, count + 1):
        customer_id, country = rng.choice(people)
        items = [{"product": name, "category": category, "quantity": rng.randint(1, 3),
                  "price": round(price * rng.uniform(0.9, 1.1), 2)}
                 for name, category, price in rng.sample(PRODUCTS, rng.randint(1, 3))]
        docs.append({
            "order_number": number,
            "customer_id": customer_id,
            "country": country,
            "city": rng.choice(COUNTRIES[country]),
            "status": rng.choices(STATUSES, weights=[5, 10, 15, 62, 8])[0],
            "items": items,
            "total": round(sum(i["quantity"] * i["price"] for i in items), 2),
            "created_at": NOW - timedelta(minutes=rng.randint(0, 365 * 24 * 60)),
        })
    return docs


client = get_client()
db = client[MONGO_DB]

title("00 - Seed de la colección fragmentada orders")

step(f"Borrando la base '{MONGO_DB}' (si existe)")
client.drop_database(MONGO_DB)

step("enableSharding: crear la base con shard1rs como primary shard")
client.admin.command("enableSharding", MONGO_DB, primaryShard="shard1rs")

step("shardCollection: orders con shard key por rango {country: 1, customer_id: 1}")
client.admin.command("shardCollection", f"{MONGO_DB}.orders", key={"country": 1, "customer_id": 1})
print("MongoDB crea automáticamente un índice sobre la shard key.")

step("Insertando 20.000 órdenes a través de mongos")
db.orders.insert_many(orders(), ordered=False)
db.orders.create_index("order_number", unique=False)
print("órdenes:", db.orders.estimated_document_count())

step("Distribución inmediatamente después de insertar")
table(distribution(client))
print("La colección empezó con un solo chunk (todo el rango de la shard key) en el primary shard, así que")
print("todo cayó en shard1rs. El balancer lo detecta y mueve rangos a shard2rs en segundo plano.")

step("Esperando a que el balancer reparta los datos (revisión cada 5 s, máx. 2 min)")
for _ in range(24):
    time.sleep(5)
    rows = distribution(client)
    print("  " + " | ".join(f"{r['shard']}: {r['documents']:>6} docs {r['MB']:.1f} MB" for r in rows))
    if len(rows) == 2 and abs(rows[0]["MB"] - rows[1]["MB"]) < 3:
        break
print("Listo: los datos quedaron repartidos entre los dos shards. Siguiente: python lessons/01_crud.py")
