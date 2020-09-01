const {Kafka} = require('kafkajs');
const kafka = new Kafka({
    clientId: 'client-dev-dummy',
    brokers: [`${process.env.KAFKA_HOST}`]
})


const DummyConsumer = async () => {
    const consumer = kafka.consumer({groupId: 'dummy-group-id'})
    await consumer.connect()
    await consumer.subscribe({topic: 'products', fromBeginning: true});

    await consumer.run({
        eachBatchAutoResolve: false,
        eachBatch: async ({batch, resolveOffset, heartbeat, isRunning, isStale}) => {
            for (let message of batch.messages) {
                if (!isRunning() || isStale()) break

                try {
                    // console.log('message', JSON.parse(message.value.toString()));
                    const dataFromKafka = JSON.parse(message.value.toString());
                    console.log(dataFromKafka, '******dataFromKafka*****');
                } catch (e) {
                    console.log(e);
                }

                resolveOffset(message.offset);
                await heartbeat();
            }
        }
    })

}

exports.DummyConsumer = DummyConsumer;
