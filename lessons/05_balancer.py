# Lección 05 - Balancer y migraciones de chunks.
#
# El balancer corre en el config server: si un shard tiene bastante más datos que otro (más de ~3 veces el
# tamaño de chunk), mueve rangos de la shard key hasta equilibrarlos. También se puede cortar (split) y
# mover (moveRange) un rango a mano, por ejemplo para dejar todos los datos de un país en un mismo shard.
from bson import MinKey
from pymongo.errors import OperationFailure

from lab.client import get_client
from lab.cluster import chunks, distribution, route
from lab.config import MONGO_DB
from lab.show import show, step, table, title

client = get_client()
db = client[MONGO_DB]

title("05 - Balancer y migraciones")

step("Estado del balancer")
status = client.admin.command("balancerStatus")
show({"mode": status["mode"], "inBalancerRound": status["inBalancerRound"],
      "chunksize_MB": client.config.settings.find_one({"_id": "chunksize"})["value"]})

step("Distribución actual")
table(distribution(client))
print("Chunks por shard:", {s: sum(1 for c in chunks(client) if c["shard"] == s) for s in ("shard1rs", "shard2rs")})

step("Migración manual: mover todas las órdenes de Colombia a un solo shard")
namespace = f"{MONGO_DB}.orders"
before = route(db, {"country": "Colombia"})["shards"]
target = "shard2rs" if before != "shard2rs" else "shard1rs"
print(f"Colombia vive hoy en: {before}  ->  destino: {target}")

# 1. split: cortar los chunks justo donde empieza Colombia y donde empieza el país siguiente
next_country = min(c for c in db.orders.distinct("country") if c > "Colombia")
for point in ({"country": "Colombia", "customer_id": MinKey()}, {"country": next_country, "customer_id": MinKey()}):
    try:
        client.admin.command("split", namespace, middle=point)
    except OperationFailure:
        pass  # ese punto ya era un límite de chunk

# 2. moveRange: mover cada chunk de Colombia que no esté en el destino
for chunk in chunks(client):
    if chunk["min"]["country"] == "Colombia" and chunk["shard"] != target:
        client.admin.command("moveRange", namespace, min=chunk["min"], max=chunk["max"], toShard=target)

print(f"Después de la migración, una consulta por Colombia va a: {route(db, {'country': 'Colombia'})['shards']}")
print("mongos se enteró por los metadatos del config server; la aplicación no cambió nada.")

step("Chunks después de la migración")
table({"shard": c["shard"], "desde": str(c["min"]), "hasta": str(c["max"])} for c in chunks(client))
print("Si la diferencia entre shards vuelve a superar ~3 chunks, el balancer moverá otros rangos para compensar.")

step("Comandos útiles en mongosh")
print("  sh.status()                       resumen del clúster")
print("  db.orders.getShardDistribution()  datos por shard")
print("  sh.stopBalancer() / sh.startBalancer()")
