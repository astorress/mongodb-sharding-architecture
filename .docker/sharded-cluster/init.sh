#!/bin/bash
set -e

# Every shard and the config server is a single-member replica set
initiate() {
  local host=$1
  local config=$2
  echo "Initiating $host"
  mongosh "mongodb://$host/?directConnection=true" --quiet --eval "
    try {
      rs.status();
      print('  already initiated');
    } catch (e) {
      if (e.codeName !== 'NotYetInitialized') throw e;
      rs.initiate($config);
    }"
  until mongosh "mongodb://$host/?directConnection=true" --quiet --eval "db.hello().isWritablePrimary" | grep -q true; do
    sleep 1
  done
}

initiate configsvr:27019 "{ _id: 'configrs', configsvr: true, members: [{ _id: 0, host: 'configsvr:27019' }] }"
initiate shard1:27018 "{ _id: 'shard1rs', members: [{ _id: 0, host: 'shard1:27018' }] }"
initiate shard2:27018 "{ _id: 'shard2rs', members: [{ _id: 0, host: 'shard2:27018' }] }"

echo "Waiting for mongos (it answers once the config server is initiated)"
until mongosh "mongodb://mongos:27017" --quiet --eval "db.adminCommand('ping').ok" 2>/dev/null | grep -q 1; do
  sleep 1
done

echo "Registering the shards in mongos"
mongosh "mongodb://mongos:27017" --quiet --eval "
  for (const [name, host] of [['shard1rs', 'shard1:27018'], ['shard2rs', 'shard2:27018']]) {
    if (db.getSiblingDB('config').shards.findOne({ _id: name })) {
      print('  ' + name + ' already added');
    } else {
      sh.addShard(name + '/' + host);
      print('  ' + name + ' added');
    }
  }
  // Small range size (MB) so the balancer moves data even with a small dataset (default: 128 MB)
  db.getSiblingDB('config').settings.updateOne({ _id: 'chunksize' }, { \$set: { value: 1 } }, { upsert: true });
  print('  chunksize = 1 MB');"

echo "Sharded cluster ready"
