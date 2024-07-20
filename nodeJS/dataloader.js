const { createClient } = require('redis');
const fs = require('fs');
const readline = require('readline');
const { redisClient } = require('./connection')


async function loadSampleData() {
    await redisClient.connect();

    await redisClient.flushDb();

    const pipeline = redisClient.multi();

    pipeline.zAdd("_SAM_:ABC#123", [
        { score: 10, value: 'XYZ#{123}+789' },
        { score: 20, value: 'XYZ#{345}+123' }
    ]);

    pipeline.set("_SAM_:XYZ#{123}+789", '{"PK":"123","SK":"ABC#123","assetId":"123","totalDuration":1000000,"position":100000,"created_at":1658910745098,"updated_at":1658910745098,"isWatching":true,"isOnAir":true,"language":"","Data":"ABC#123","episodeNumber":1,"showId":123,"seasonId":123}');

    pipeline.set("_SAM_:XYZ#{345}+123", '{"PK":"456","SK":"ABC#231","assetId":"124","totalDuration":1000000,"position":100000,"created_at":1658910745098,"updated_at":1658910745098,"isWatching":true,"isOnAir":true,"language":"","Data":"ABC#325","episodeNumber":2,"showId":123,"seasonId":123}');

    pipeline.hSet("_SAM_:sh_0000000000000000000_000000000", {
        cluster: '{"cluster_id": 00}',
        profile: '{"age": "Adult", "gender": "gender", "top_genre": ["genre"], "top_region": ["Region"]}'
    });

    pipeline.hSet("_SAM_:sh_cluster_00", {
        shortinfodata: '{"0000": {"stackName": "cluster_00_stack_1", "description": "This is cluster description", "firstContent": 11111111111, "stackScore1": 11111, "stackScore2": 22222, "stackScore3": 33333, "hashvalue": "132323234hjhfj83923832nksf900sfs", "hashtimestamp": "12222222222222"}}',
        reco: '{"0000": [1111111111, 22222222222, 3333333333333, 444444444444, 555555555555]}'
    });

    pipeline.hSet("_SAM_:0000000000000000000_000000000", {
        cos: '{"livo": "111111111111,222222222222", "livs": "111111111111,222222222222"}',
        profile: '{"top_genre": ["genre"], "top_language": ["language"], "top_timecategory": null, "top_region": ["country"], "watched": [{"user_category": "original", "content": ["11111111111_6"]}]}',
        clb: '{"livo": "111111111111,222222222222", "livs": "111111111111,222222222222"}',
        sri: '{"livo": "111111111111,222222222222", "livs": "111111111111,222222222222"}',
        cldlng: '{"livo": "111111111111,222222222222", "livs": "111111111111,222222222222"}'
    });

    pipeline.hSet("_SAM_:country_region_age_gender", {
        livo: '{"language1":[111111111111,222222222222],"language2":[111111111111,222222222222]}',
        sport: '{"language1":[111111111111,222222222222]}',
        s: '{"language1":[111111111111,222222222222],"language2":[111111111111,222222222222]}',
        livs: '{"language1":[111111111111,222222222222],"language2":[111111111111,222222222222]}',
        mov: '{"language1":[111111111111,222222222222],"language2":[111111111111,222222222222]}'
    });

    pipeline.hSet("_SAM_:sh_region_platform", {
        shortinfodata: '{"0000": {"stackName": "stack_1", "description": "This is stack description", "firstContent": 11111111111, "stackScore1": 11111, "stackScore2": 22222, "stackScore3": 33333, "hashvalue": "132323234hjhfj83923832nksf900sfs", "hashtimestamp": "12222222222222"}}',
        reco: '{"0000": [1111111111, 22222222222, 3333333333333, 444444444444, 555555555555]}'
    });

    pipeline.hSet("_SAM_:country_region_platform", {
        livo: '{"language1": [11111111111], "language2": [2222222222], "language3": [3333333333]}',
        s: '{"language1": [11111111111], "language2": [2222222222], "language3": [3333333333]}',
        livs: '{"language1": [11111111111], "language2": [2222222222], "language3": [3333333333]}',
        mov: '{"language1": [11111111111], "language2": [2222222222], "language3": [3333333333]}'
    });

    pipeline.set('_SAM_:DUMMY_TEXT', "This is a dummy text just to simulate a 'long running textual description' of anything. This is being used for testing purposes so as to estimate how much memory will be optimised if the same text will be compressed using some algorithm in different programming languages for example: Java, Python, Go, Javascript.This is a dummy text just to simulate a 'long running textual description' of anything. This is being used for testing purposes so as to estimate how much memory will be optimised if the same text will be compressed using some algorithm in different programming languages for example: Java, Python, Go, Javascript.This is a dummy text just to simulate a 'long running textual description' of anything. This is being used for testing purposes so as to estimate how much memory will be optimised if the same text will be compressed using some algorithm in different programming languages for example: Java, Python, Go, Javascript.This is a dummy text just to simulate a 'long running textual description' of anything. This is being used for testing purposes so as to estimate how much memory will be optimised if the same text will be compressed using some algorithm in different programming languages for example: Java, Python, Go, Javascript.This is a dummy text just to simulate a 'long running textual description' of anything. This is being used for testing purposes so as to estimate how much memory will be optimised if the same text will be compressed using some algorithm in different programming languages for example: Java, Python, Go, Javascript.");
    
    /**
     * We can send the command one at a time using:
     * 
    client.hSet("sh_0000000000000000000_000000000", {
        cluster: '{"cluster_id": 00}',
        profile: '{"age": "Adult", "gender": "gender", "top_genre": ["genre"], "top_region": ["Region"]}'
    });
     *
     */

    const results = await pipeline.exec();
    //console.log('Execution results:', results);
}

module.exports = {
    loadSampleData
};