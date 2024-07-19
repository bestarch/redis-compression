const { createClient } = require('redis');

async function saveAndRetrieveBytes() {
    // Create a Redis client
    const client  = createClient({
        url: 'redis://localhost:6379', 
        password: 'admin',
        
        
    });

    client.on('error', (err) => {
        console.error('Redis Client Error', err);
    });

    await client.connect();

    // Example binary data
    //const data = Buffer.alloc(2) 
    const data = Buffer.from("ykgvdsfbvld gvoidubr vigguerbfygres uerbiuefyir uiryguetygur ")
    //data.write("Hello")

    // Save binary data to Redis
    await client.set('binaryKey', data);

    // Retrieve binary data from Redis
    const result = await client.get('binaryKey');

    console.log('Retrieved binary data:', result);

    // Disconnect the client
    await client.disconnect();
}

saveAndRetrieveBytes();