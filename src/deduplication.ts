import crypto from "node:crypto";
import { ObliviousSet } from "oblivious-set";
import pino from "pino";
import type Pulsar from "pulsar-client";
import stringify from "safe-stable-stringify";
import type { DeduplicationConfig } from "./config";

export const createHasher = (
  ignoredProperties: string[],
): ((message: Pulsar.Message) => Buffer) => {
  const ignored = new Set(ignoredProperties);
  const calculateHash = (message: Pulsar.Message): Buffer => {
    const properties = message.getProperties();
    const keptProperties = Object.fromEntries(
      Object.entries(properties).filter(([key]) => !ignored.has(key)),
    );
    const deterministicPropertyBuffer = Buffer.from(
      stringify(keptProperties),
      "utf8",
    );
    // Ignore the event timestamp of the message as it is likely different for
    // each data source replica.
    const toHash = Buffer.concat([
      message.getData(),
      deterministicPropertyBuffer,
    ]);
    /**
     * Blake2b of 64 bytes is probably overkill. We do not need a
     * cryptographically strong hash function to filter out duplicates, just a
     * collision-resistant one. For example the 128-bit xxHash would probably
     * do the trick. Yet finding a dependency to rely on long-term with minimal
     * maintenance is not as trivial as just using Node.js and OpenSSL.
     */
    return crypto.createHash("BLAKE2b512").update(toHash).digest();
  };
  return calculateHash;
};

export const keepDeduplicating = async (
  logger: pino.Logger,
  producer: Pulsar.Producer,
  consumer: Pulsar.Consumer,
  { deduplicationWindowInSeconds, ignoredProperties }: DeduplicationConfig,
) => {
  const calculateHash = createHasher(ignoredProperties);
  const cache = new ObliviousSet(deduplicationWindowInSeconds * 1e3);
  /* eslint-disable no-await-in-loop */
  for (;;) {
    const message = await consumer.receive();
    const hash = calculateHash(message);
    if (!cache.has(hash)) {
      const digest = hash.toString("hex");
      logger.debug(
        {
          messageData: message.getData(),
          messageProperties: message.getProperties(),
          messageEventTimestamp: message.getEventTimestamp(),
          messagePublishTimestamp: message.getPublishTimestamp(),
          digest,
          cacheSize: cache.map.size,
        },
        "Got a new message",
      );
      cache.add(hash);
      // In case of an error, exit via the listener on unhandledRejection.
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      producer
        .send({
          data: message.getData(),
          properties: {
            ...message.getProperties(),
            ...{ digest },
          },
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
