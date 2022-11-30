import _sodium from "libsodium-wrappers";
import { ObliviousSet } from "oblivious-set";
import type Pulsar from "pulsar-client";
import stringify from "safe-stable-stringify";
import type { DeduplicationConfig } from "./config";

const createHasher = async (
  ignoredProperties: string[]
): Promise<(message: Pulsar.Message) => Uint8Array> => {
  await _sodium.ready;
  const sodium = _sodium;
  const hashKey = sodium.randombytes_buf(sodium.crypto_shorthash_KEYBYTES);
  const ignored = new Set(ignoredProperties);
  const calculateHash = (message: Pulsar.Message): Uint8Array => {
    const properties = message.getProperties();
    const keptProperties = Object.fromEntries(
      Object.entries(properties).filter(([key]) => !ignored.has(key))
    );
    const deterministicPropertyBuffer = Buffer.from(stringify(keptProperties));
    // Ignore the event timestamp of the message as it is likely different for
    // each data source replica.
    const toHash = Buffer.concat([
      message.getData(),
      deterministicPropertyBuffer,
    ]);
    return sodium.crypto_shorthash(toHash, hashKey);
  };
  return calculateHash;
};

const keepDeduplicating = async (
  producer: Pulsar.Producer,
  consumer: Pulsar.Consumer,
  { deduplicationWindowInSeconds, ignoredProperties }: DeduplicationConfig
) => {
  const calculateHash = await createHasher(ignoredProperties);
  const cache = new ObliviousSet(deduplicationWindowInSeconds * 1e3);
  /* eslint-disable no-await-in-loop */
  for (;;) {
    const message = await consumer.receive();
    const hash = calculateHash(message);
    if (!cache.has(hash)) {
      cache.add(hash);
      // In case of an error, exit via the listener on unhandledRejection.
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      producer
        .send({
          data: message.getData(),
          properties: message.getProperties(),
          eventTimestamp: message.getEventTimestamp(),
        })
        .then(() => {
          // In case of an error, exit via the listener on unhandledRejection.
          // eslint-disable-next-line @typescript-eslint/no-floating-promises
          consumer.acknowledge(message).then(() => {});
        });
    }
  }
  /* eslint-enable no-await-in-loop */
};

export default keepDeduplicating;
