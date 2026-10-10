# Lección 03 - Agregaciones en un clúster fragmentado.
#
# mongos divide el pipeline: cada shard ejecuta en paralelo lo que puede con sus datos ($match, un $group
# parcial...) y luego se fusionan los resultados parciales.
from lab.client import get_db
from lab.show import show, step, table, title

db = get_db()
orders = db.orders

title("03 - Agregaciones")

step("Ventas por país ($match -> $group -> $sort)")
table(orders.aggregate([
    {"$match": {"status": {"$ne": "cancelled"}}},
    {"$group": {"_id": "$country", "orders": {"$sum": 1}, "revenue": {"$sum": "$total"}, "avg": {"$avg": "$total"}}},
    {"$sort": {"revenue": -1}},
    {"$project": {"_id": 0, "country": "$_id", "orders": 1, "revenue": 1, "avg": 1}},
]), ["country", "orders", "revenue", "avg"])

step("Unidades vendidas por categoría ($unwind de los ítems)")
table(orders.aggregate([
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.category", "units": {"$sum": "$items.quantity"}}},
    {"$sort": {"units": -1}},
    {"$project": {"_id": 0, "category": "$_id", "units": 1}},
]), ["category", "units"])

step("Órdenes por mes y estado ($dateTrunc + $group por dos campos), últimos 3 meses")
table(orders.aggregate([
    {"$group": {"_id": {"month": {"$dateToString": {"date": "$created_at", "format": "%Y-%m"}}, "status": "$status"},
                "orders": {"$sum": 1}}},
    {"$sort": {"_id.month": -1, "orders": -1}},
    {"$limit": 15},
    {"$project": {"_id": 0, "month": "$_id.month", "status": "$_id.status", "orders": 1}},
]), ["month", "status", "orders"])

step("Top 5 clientes de Colombia por gasto (pipeline dirigido a un shard: filtra por el prefijo de la shard key)")
table(orders.aggregate([
    {"$match": {"country": "Colombia"}},
    {"$group": {"_id": "$customer_id", "orders": {"$sum": 1}, "spent": {"$sum": "$total"}}},
    {"$sort": {"spent": -1}},
    {"$limit": 5},
    {"$project": {"_id": 0, "customer_id": "$_id", "orders": 1, "spent": 1}},
]), ["customer_id", "orders", "spent"])

step("¿Cómo dividió mongos el pipeline 'ventas por estado'? (explain)")
explain = db.command("explain", {"aggregate": "orders", "cursor": {}, "pipeline": [
    {"$group": {"_id": "$status", "orders": {"$sum": 1}}}]}, verbosity="queryPlanner")
show({
    "shards consultados": sorted(explain["shards"]),
    "en cada shard (shardsPart)": explain["splitPipeline"]["shardsPart"],
    "al fusionar (mergerPart)": [list(stage)[0] for stage in explain["splitPipeline"]["mergerPart"]],
})
print("Cada shard cuenta sus propias órdenes por estado; luego se suman los conteos parciales.")
