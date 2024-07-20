const { createClient } = require('redis');

const redisClient = createClient({
    url: 'redis://localhost:6379', 
    password: 'admin'
});

redisClient.on('error', (err) => {
    console.error('Redis not connected', err);
});


module.exports = {
    redisClient
};