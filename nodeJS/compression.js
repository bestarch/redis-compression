const { commandOptions } = require('redis')
const { loadSampleData } = require('./dataloader')
const { redisClient } = require('./connection')
const { compress } = require('./compress')

async function convertAndCompress() {
    await loadSampleData();
    const before = (await redisClient.info('memory')).split('\n')[1].split(':')[1]
    console.log("Memory size before :: "+ before)
    let cursor = 0;
    const keys = [];

    do {
        const result = await redisClient.scan(cursor, {
            MATCH: "_SAM_:*",
            COUNT: 100
        });
        cursor = result.cursor;
        keys.push(...result.keys);
    } while (cursor != '0');

    console.log('Scanned keys:', keys);

    for (let key of keys) {
        const type = await redisClient.TYPE(key);
        let max_size = 1
        switch (type) {
            case "hash":
                const hset = await redisClient.hGetAll(commandOptions({ returnBuffers: true }), key)
                for (const elem in hset) {
                    const {compressed, size} = compress(hset[elem])
                    await redisClient.hSet(key, elem, compressed)
                    max_size = Math.max(max_size, size)
                }
                await redisClient.hSet(key, "_metadata", max_size)
                break;

            case "zset":
                const members = await redisClient.zRangeWithScores(commandOptions({ returnBuffers: true }), key, 0, -1);
                await redisClient.del(key)
                for (const member of members) {
                    const {compressed, size} = compress(member.value)
                    await redisClient.zAdd(key, {score: member.score, value: compressed})
                    max_size = Math.max(max_size, size)
                }
                await redisClient.set("_metadata"+key, max_size)
                break;

            case "string":
                const val = await redisClient.get(commandOptions({ returnBuffers: true }), key);
                const {compressed, size} = compress(val)
                await redisClient.set(key, compressed)
                await redisClient.set("_metadata"+key, size)
                break;
        
            default:
                break;
        }
    }

    console.log('Existing data successfully converted and compressed');
    const after = (await redisClient.info('memory')).split('\n')[1].split(':')[1]
    console.log("Memory size after :: "+ after)
    console.log("Difference in KB:: "+ ((parseInt(before,10)-parseInt(after,10))/1024))
    redisClient.disconnect();
}

convertAndCompress();