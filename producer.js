const { Kafka, kafka } = require('./client')
const readline = require('readline')

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

async function init() {
    const producer = kafka.producer();

    console.log('Producer connecting')
    await producer.connect();
    console.log('Producer connection success');

    rl.setPrompt('> ')
    rl.prompt();

    rl.on('line', async function (line) {
        const [riderName, location] = line.split(' ')
        await producer.send({
            topic: "ride-updates",
            messages: [
                {
                    partition: location.toLowerCase() === "north" ? 0 : 1,
                    key: 'location-update',
                    value: JSON.stringify({
                        name: riderName, loc: location
                    }),
                },

            ],
        });
    }).on('close', async () => {
        console.log("Producer disconnect")
        await producer.disconnect();
    })





}

init();

