import crypto from "node:crypto";
import { ObliviousSet } from "oblivious-set";
import pino from "pino";
import type Pulsar from "pulsar-client";
import stringify from "safe-stable-stringify";
import type { CacheRebuildConfig, DeduplicationConfig } from "./config";
import { buildUpCache } from "./cacheBuilding";

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

export const sendAndAck = async (
  logger: pino.Logger,
  producer: Pulsar.Producer,
  consumer: Pulsar.Consumer,
  producerMessage: Pulsar.ProducerMessage,
  consumerMessage: Pulsar.Message,
  onSuccess: () => void,
) => {
  await producer.send(producerMessage);
  await consumer.acknowledge(consumerMessage);
  onSuccess();
  logger.debug(
    {
      sourceTopic: consumerMessage.getTopicName(),
      destTopic: producer.getTopic(),
      payloadBytes: producerMessage.data.length,
    },
    "Published message to Pulsar producer topic",
  );
};

export const keepDeduplicating = async (
  logger: pino.Logger,
  producer: Pulsar.Producer,
  cacheReader: Pulsar.Reader,
  consumer: Pulsar.Consumer,
  cacheRebuildConfig: CacheRebuildConfig,
  { deduplicationWindowInSeconds, ignoredProperties }: DeduplicationConfig,
) => {
  logger.info(
    { deduplicationWindowInSeconds, ignoredProperties },
    "Some relevant configuration values",
  );
  const calculateHash = createHasher(ignoredProperties);
  const cache = new ObliviousSet(deduplicationWindowInSeconds * 1e3);
  logger.info("Build up the cache from already published messages");
  await buildUpCache(logger, cache, cacheReader, cacheRebuildConfig);
  logger.info("Handle new messages from now on");
  const logIntervalInSeconds = 60;
  const counters = { nRecentForwarded: 0, nRecentDropped: 0 };
  setInterval(() => {
    logger.info(
      {
        nRecentForwarded: counters.nRecentForwarded,
        nRecentDropped: counters.nRecentDropped,
      },
      "messages forwarded to destination Pulsar topic",
    );
    counters.nRecentForwarded = 0;
    counters.nRecentDropped = 0;
  }, 1_000 * logIntervalInSeconds);
  const incrementForwarded = () => {
    counters.nRecentForwarded += 1;
  };
  /* eslint-disable no-await-in-loop */
  for (;;) {
    const message = await consumer.receive();
    logger.debug(
      {
        topic: message.getTopicName(),
        messageId: message.getMessageId().toString(),
        redeliveryCount: message.getRedeliveryCount(),
        payloadBytes: message.getData().length,
        eventTimestamp: message.getEventTimestamp(),
      },
      "Received message from Pulsar consumer",
    );
    const hash = calculateHash(message);
    const digest = hash.toString("hex");
    if (!cache.has(digest)) {
      cache.add(digest);
      const producerMessage = {
        data: message.getData(),
        properties: {
          ...message.getProperties(),
          ...{ origin: JSON.stringify([digest]) },
        },
        eventTimestamp: message.getEventTimestamp(),
      };
      // To utilize concurrency and to not limit throughput unnecessarily, we
      // should _not_ await sendAndAck. Instead, Promises are handled in order
      // by Node.js. Therefore we can receive the next Pulsar message right
      // away.
      //
      // In case of an error, exit via the listener on unhandledRejection.
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      sendAndAck(
        logger,
        producer,
        consumer,
        producerMessage,
        message,
        incrementForwarded,
      );
    } else {
      // For increased throughput, we should not await here either.
      //
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      consumer.acknowledge(message);
      counters.nRecentDropped += 1;
      logger.debug(
        {
          topic: message.getTopicName(),
          messageId: message.getMessageId().toString(),
        },
        "Acknowledged duplicate message; not forwarding",
      );
    }
  }
  /* eslint-enable no-await-in-loop */
};
