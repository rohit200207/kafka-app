const {kafka} = require('./client')
const group =process.argv[2];

async function init(){

    const consumer = kafka.consumer( {groupId: group})

    console.log('consumer connecting')
    await consumer.connect();
    console.log('consumer connection success');

    await consumer.subscribe({ topics: ["ride-updates"], fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
          console.log(`${group}: [${topic}]: PART:${partition}:`,message.value.toString());
        },
    });

    //console.log('consumer disconnect');
   //// await consumer.disconnect();

  //  console.log('consumer connection success');

}

init();