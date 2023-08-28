import type { ObliviousSet } from "oblivious-set";
import type pino from "pino";
import type Pulsar from "pulsar-client";
import { CacheRebuildConfig } from "./config";

export const getDigests = (
  logger: pino.Logger,
  message: Pulsar.Message,
): string[] => {
  let result: string[] = [];
  const propertyName = "origin";
  const properties = { ...message.getProperties() };
  const origin = properties[propertyName];
  if (origin == null) {
    logger.warn(
      {
        messageId: message.getMessageId(),
        eventTimestamp: message.getEventTimestamp(),
        publishTimestamp: message.getPublishTimestamp(),
        topic: message.getTopicName(),
        properties: { ...properties },
      },
      `While reading the producer topic to build a cache, a message without the property "${propertyName}" was found. Skipping the message from caching.`,
    );
  } else {
    let digests: string[] | undefined;
    try {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
      digests = JSON.parse(origin);
      if (
        !(
          Array.isArray(digests) &&
          digests.every((s: string) => typeof s === "string" && s !== "")
        )
      ) {
        logger.warn(
          {
            messageId: message.getMessageId(),
            eventTimestamp: message.getEventTimestamp(),
            publishTimestamp: message.getPublishTimestamp(),
            topic: message.getTopicName(),
            properties: { ...properties },
          },
          `While reading the producer topic to build a cache, a message was found where the property "${propertyName}" was not a JSON array of non-empty strings. Skipping the message from caching.`,
        );
      } else {
        result = digests;
      }
    } catch {
      logger.warn(
        {
          messageId: message.getMessageId(),
          eventTimestamp: message.getEventTimestamp(),
          publishTimestamp: message.getPublishTimestamp(),
          topic: message.getTopicName(),
          properties: { ...properties },
        },
        `While reading the producer topic to build a cache, a message was found where the property "${propertyName}" was not JSON. Skipping the message from caching.`,
      );
    }
  }
  return result;
};

export const buildUpCache = async (
  logger: pino.Logger,
  cache: ObliviousSet,
  cacheReader: Pulsar.Reader,
  { cacheWindowInSeconds }: CacheRebuildConfig,
): Promise<void> => {
  const now = Date.now();
  const startingTime = now - 1_000 * cacheWindowInSeconds;
  await cacheReader.seekTimestamp(startingTime);
  /* eslint-disable no-await-in-loop */
  while (cacheReader.hasNext()) {
    const oldMessage = await cacheReader.readNext();
    const digests = getDigests(logger, oldMessage);
    digests.forEach((digest) => cache.add(digest));
  }
  /* eslint-enable no-await-in-loop */
  await cacheReader.close();
};
