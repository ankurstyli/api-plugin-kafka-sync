import pkg from "../package.json";
import productProducer from './producer/updateProduct.js'
import dummyConsumer from './consumer/dummyConsumer.js'
/**
 * @summary Import and call this function to add this plugin to your API.
 * @param {ReactionAPI} app The ReactionAPI instance
 * @returns {undefined}
 */
export default async function register(app) {
  await app.registerPlugin({
    label: "Kafka Sync",
    name: "kafka-sync",
    version: pkg.version
  });
  await dummyConsumer();

}
