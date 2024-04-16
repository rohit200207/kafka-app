
const { Kafka, kafka } =require('./client')




async function init() {
    const admin = kafka.admin();
    console.log('Admin connecting')
    admin.connect();
    console.log('Admin connection success');

    console.log("Creating topic [rider-updates]")

    await admin.createTopics({
        topics: [{
            topic: "ride-updates",
            numPartitions: 2,

        },
        ],
    });

    console.log("Topic created success")

    console.log("admin disconnecting")
    await admin.disconnect();

}

init();