from lab.config import MONGO_DB


def route(db, query, collection="orders"):
    # explain through mongos says which shards were asked: SINGLE_SHARD (targeted) or SHARD_MERGE (broadcast)
    explain = db.command("explain", {"find": collection, "filter": query}, verbosity="queryPlanner")
    winning = explain["queryPlanner"]["winningPlan"]
    return {"router": winning["stage"], "shards": ", ".join(sorted(s["shardName"] for s in winning["shards"]))}


def distribution(client, collection="orders"):
    rows = client.admin.aggregate([{"$shardedDataDistribution": {}}, {"$match": {"ns": f"{MONGO_DB}.{collection}"}}])
    return sorted(({"shard": s["shardName"], "documents": s["numOwnedDocuments"],
                    "MB": s["ownedSizeBytes"] / 1024 / 1024} for d in rows for s in d["shards"]),
                  key=lambda r: r["shard"])


def chunks(client, collection="orders"):
    return list(client.config.collections.aggregate([
        {"$match": {"_id": f"{MONGO_DB}.{collection}"}},
        {"$lookup": {"from": "chunks", "localField": "uuid", "foreignField": "uuid", "as": "chunk"}},
        {"$unwind": "$chunk"},
        {"$sort": {"chunk.min": 1}},
        {"$project": {"_id": 0, "shard": "$chunk.shard", "min": "$chunk.min", "max": "$chunk.max"}},
    ]))
