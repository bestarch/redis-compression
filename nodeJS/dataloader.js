const { createClient } = require('redis');
const fs = require('fs');
const readline = require('readline');

// Create a Redis client
const client = createClient({
    url: 'redis://localhost:6379', // Replace with your Redis server URL and port
});

client.on('error', (err) => {
    console.error('Redis Client Error', err);
});

async function executeRedisCommands(filePath) {
    await client.connect();

    // Create a Readable stream from the file
    const fileStream = fs.createReadStream(filePath);

    // Create an interface for reading the file line by line
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    // Read each line from the file
    for await (const line of rl) {
        // Ignore empty lines
        if (line.trim()) {
            try {
                // Execute the command using the Redis client
                const [command, ...args] = line.split(' ');
                const result = await client.sendCommand([command.toUpperCase(), ...args]);
                console.log(`Result for "${line}": ${result}`);
            } catch (error) {
                console.error(`Error executing command "${line}": ${error.message}`);
            }
        }
    }

    // Disconnect the Redis client
    await client.quit();
}

// Path to the file containing Redis commands
const filePath = 'path/to/your/commands.txt';
executeRedisCommands(filePath);