# Lección 04 - Los metadatos del clúster: lo que guarda el config server y usa mongos para enrutar.
from lab.client import get_client
from lab.cluster import chunks, distribution
from lab.config import MONGO_DB
from lab.show import step, table, title

client = get_client()
config = client.config

title("04 - Metadatos del clúster (base config)")

step("Shards registrados (listShards)")
table([{"shard": s["_id"], "host": s["host"]} for s in client.admin.command("listShards")["shards"]])

step("Bases de datos y su primary shard (config.databases)")
table(config.databases.find({}, {"_id": 1, "primary": 1}))
print("Las colecciones NO fragmentadas de una base viven completas en su primary shard.")

step("Colecciones fragmentadas y su shard key (config.collections)")
table(config.collections.find({"_id": {"$regex": f"^{MONGO_DB}\\."}}, {"_id": 1, "key": 1}))

step("Chunks de orders: cada rango de la shard key y el shard dueño (config.chunks)")
table({"shard": c["shard"], "desde": str(c["min"]), "hasta": str(c["max"])} for c in chunks(client))
print("MinKey/MaxKey son los extremos: menor y mayor que cualquier valor posible.")

step("Datos por shard ($shardedDataDistribution)")
table(distribution(client))

step("Routers conectados (config.mongos)")
table(config.mongos.find({}, {"_id": 1, "mongoVersion": 1}))
