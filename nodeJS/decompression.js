const { commandOptions } = require('redis')
const { redisClient } = require('./connection')
const { deCompress } = require('./compress')

async function decompressAndStringify() {
    let cursor = 0;
    const keys = [];
    await redisClient.connect();
    do {
        const result = await redisClient.scan(cursor, {
            MATCH: "_SAM_:*",
            COUNT: 100
        });
        cursor = result.cursor;
        keys.push(...result.keys);
    } while (cursor != '0');

    console.log('Scanned keys:', keys);
    let size = 1;
    for (let key of keys) {
        const type = await redisClient.TYPE(key);
        switch (type) {
            case "hash":
                size = await redisClient.hGet(key, "_metadata")
                await redisClient.hDel(key, "_metadata")
                const hset = await redisClient.hGetAll(commandOptions({ returnBuffers: true }), key)
                for (const elem in hset) {
                    const decompressed = deCompress(hset[elem], parseInt(size, 10))
                    await redisClient.hSet(key, elem, decompressed)
                    console.log("Key -> "+key+"; Retrieved element -> " + decompressed)
                }
                console.log("\n")
                break;

            case "zset":
                size = await redisClient.get("_metadata"+key)
                const members = await redisClient.zRangeWithScores(commandOptions({ returnBuffers: true }), key, 0, -1);
                await redisClient.del(key)
                for (const member of members) {
                    const decompressed = deCompress(member.value, parseInt(size, 10))
                    await redisClient.zAdd(key, {score: member.score, value: decompressed})
                    console.log("Key -> "+key+"; Retrieved sortedset member -> " + decompressed)
                }
                await redisClient.del("_metadata"+key)
                console.log("\n")
                break;

            case "string":
                size = await redisClient.get("_metadata"+key)
                const val = await redisClient.get(commandOptions({ returnBuffers: true }), key);
                const decompressed = deCompress(val, parseInt(size, 10))
                await redisClient.set(key, decompressed)
                await redisClient.del("_metadata"+key)
                console.log("Key -> "+key+"; Retrieved string value -> " + decompressed)
                console.log("\n")
                break;
        
            default:
                break;
        }
    }

    console.log('Existing data successfully uncompressed');
    redisClient.disconnect();
}

decompressAndStringify();