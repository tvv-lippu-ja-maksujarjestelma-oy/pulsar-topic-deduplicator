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
  const start = now - cacheWindowInSeconds * 1000;

  await cacheReader.seekTimestamp(start);

  // Before building up the deduplication cache, the cache reader is moved to the position corresponding to the start of the desired time window.
  // The following section then reads messages from the Pulsar topic starting from that position until the cache window is filled or there are no more messages.
  // Each message's relevant deduplication digests are extracted and added to the cache.
  const cutoffTs = now;

  const READ_TIMEOUT_MS = 1000; // 1s
  const MAX_CONSECUTIVE_TIMEOUTS = 3; // ~3s of emptiness → stop

  let consecutiveTimeouts = 0;
  const MAX_NON_TIMEOUT_RETRIES = 5;
  const MAX_WARMUP_MS = 60_000; // 60s budget for warm-up retries
  const BASE_BACKOFF_MS = 500;
  let nonTimeoutRetries = 0;
  const warmupStart = now;

  try {
    /* eslint-disable no-await-in-loop, no-constant-condition */
    while (true) {
      try {
        const msg = await cacheReader.readNext(READ_TIMEOUT_MS);

        const publishTs = msg.getPublishTimestamp?.() ?? 0;
        const eventTs = msg.getEventTimestamp?.() ?? 0;
        const ts = publishTs > 0 ? publishTs : eventTs;
        if (ts > cutoffTs) {
          break;
        }

        const digests = getDigests(logger, msg);
        digests.forEach((d) => cache.add(d));

        consecutiveTimeouts = 0;
      } catch (e) {
        const err = e as {
          name?: string;
          code?: unknown;
          message?: unknown;
        };
        const isTimeout =
          err?.name === "TimeoutError" ||
          err?.code === ("Timeout" as unknown) ||
          (typeof err?.message === "string" && /timeout/i.test(err.message));

        if (!isTimeout) {
          const backoffMs = Math.min(
            BASE_BACKOFF_MS * 2 ** nonTimeoutRetries,
            5_000,
          );
          logger.warn(
            { err, attempt: nonTimeoutRetries + 1, backoffMs },
            "Cache warm-up read error; retrying",
          );
          await new Promise<void>((resolve) => {
            setTimeout(resolve, backoffMs);
          });
          nonTimeoutRetries += 1;
          const elapsedMs = Date.now() - warmupStart;
          if (
            nonTimeoutRetries >= MAX_NON_TIMEOUT_RETRIES ||
            elapsedMs >= MAX_WARMUP_MS
          ) {
            logger.error(
              { err, attempts: nonTimeoutRetries, elapsedMs },
              "Cache warm-up aborted after repeated errors; proceeding without warm-up",
            );
            break;
          }
          // eslint-disable-next-line no-continue
          continue;
        }

        consecutiveTimeouts += 1;
        if (consecutiveTimeouts >= MAX_CONSECUTIVE_TIMEOUTS) {
          break;
        }
      }
    }
    /* eslint-enable no-await-in-loop, no-constant-condition */
  } finally {
    try {
      await cacheReader.close();
    } catch (err) {
      logger.warn(
        { err },
        "Failed to close Pulsar cache reader after warm-up; continuing",
      );
    }
  }
};
