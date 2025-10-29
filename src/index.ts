import type pino from "pino";
import type Pulsar from "pulsar-client";
import { getConfig } from "./config";
import { keepDeduplicating } from "./deduplication";
import { createLogger } from "./gcpLogging";
import createHealthCheckServer from "./healthCheck";
import {
  createPulsarClient,
  createPulsarProducer,
  createPulsarConsumer,
  createPulsarCacheReader,
} from "./pulsar";
import transformUnknownToError from "./util";

/**
 * Exit gracefully.
 */
const exitGracefully = async (
  logger: pino.Logger,
  exitCode: number,
  exitError?: Error,
  setHealthOk?: (isOk: boolean) => void,
  closeHealthCheckServer?: () => Promise<void>,
  client?: Pulsar.Client,
  producer?: Pulsar.Producer,
  cacheReader?: Pulsar.Reader,
  consumer?: Pulsar.Consumer,
) => {
  if (exitError) {
    logger.fatal(exitError);
  }
  logger.info("Start exiting gracefully");
  process.exitCode = exitCode;
  try {
    if (setHealthOk) {
      logger.info("Set health checks to fail");
      setHealthOk(false);
    }
  } catch (err) {
    logger.error(
      { err },
      "Something went wrong when setting health checks to fail",
    );
  }
  try {
    if (consumer) {
      logger.info("Close Pulsar consumer");
      await consumer.close();
    }
  } catch (err) {
    logger.error({ err }, "Something went wrong when closing Pulsar consumer");
  }
  try {
    if (cacheReader) {
      logger.info("Close Pulsar cache reader");
      await cacheReader.close();
    }
  } catch (err) {
    logger.error(
      { err },
      "Something went wrong when closing Pulsar cache reader",
    );
  }
  try {
    if (producer) {
      logger.info("Flush Pulsar producer");
      await producer.flush();
    }
  } catch (err) {
    logger.error({ err }, "Something went wrong when flushing Pulsar producer");
  }
  try {
    if (producer) {
      logger.info("Close Pulsar producer");
      await producer.close();
    }
  } catch (err) {
    logger.error({ err }, "Something went wrong when closing Pulsar producer");
  }
  try {
    if (client) {
      logger.info("Close Pulsar client");
      await client.close();
    }
  } catch (err) {
    logger.error({ err }, "Something went wrong when closing Pulsar client");
  }
  try {
    if (closeHealthCheckServer) {
      logger.info("Close health check server");
      await closeHealthCheckServer();
    }
  } catch (err) {
    logger.error(
      { err },
      "Something went wrong when closing health check server",
    );
  }
  logger.info("Exit process");
  process.exit(); // eslint-disable-line no-process-exit
};

/**
 * Main function.
 */
/* eslint-disable @typescript-eslint/no-floating-promises */
(async () => {
  /* eslint-enable @typescript-eslint/no-floating-promises */
  const serviceName = "pulsar-topic-deduplicator";
  try {
    const logger = createLogger({ name: serviceName });

    let setHealthOk: (isOk: boolean) => void;
    let closeHealthCheckServer: () => Promise<void>;
    let client: Pulsar.Client;
    let producer: Pulsar.Producer;
    let cacheReader: Pulsar.Reader;
    let consumer: Pulsar.Consumer;

    const exitHandler = (exitCode: number, exitError?: Error) => {
      // Exit next.
      /* eslint-disable @typescript-eslint/no-floating-promises */
      exitGracefully(
        logger,
        exitCode,
        exitError,
        setHealthOk,
        closeHealthCheckServer,
        client,
        producer,
        cacheReader,
        consumer,
      );
      /* eslint-enable @typescript-eslint/no-floating-promises */
    };

    try {
      // Handle different kinds of exits.
      process.on("beforeExit", () => exitHandler(1, new Error("beforeExit")));
      process.on("unhandledRejection", (reason) =>
        exitHandler(1, transformUnknownToError(reason)),
      );
      process.on("uncaughtException", (err) => exitHandler(1, err));
      process.on("SIGINT", (signal) => exitHandler(130, new Error(signal)));
      process.on("SIGQUIT", (signal) => exitHandler(131, new Error(signal)));
      process.on("SIGTERM", (signal) => exitHandler(143, new Error(signal)));

      logger.info(`Start service ${serviceName}`);
      logger.info("Read configuration");
      const config = getConfig(logger);
      logger.info("Create health check server");
      ({ closeHealthCheckServer, setHealthOk } = createHealthCheckServer(
        config.healthCheck,
      ));
      logger.info("Create Pulsar client");
      client = createPulsarClient(config.pulsar);
      logger.info("Create Pulsar producer");
      producer = await createPulsarProducer(client, config.pulsar);
      logger.debug(
        {
          topic: config.pulsar.producerConfig.topic,
          blockIfQueueFull: config.pulsar.producerConfig.blockIfQueueFull,
          compressionType: config.pulsar.producerConfig.compressionType,
          producerName: producer.getProducerName(),
          connected: producer.isConnected(),
        },
        "Created Pulsar producer",
      );
      logger.info("Create Pulsar cache-filling reader");
      cacheReader = await createPulsarCacheReader(client, config.pulsar);
      logger.debug(
        {
          topic: config.pulsar.cacheReaderConfig.topic,
          readerName: config.pulsar.cacheReaderConfig.readerName,
          startMessageId:
            config.pulsar.cacheReaderConfig.startMessageId.toString(),
          connected: cacheReader.isConnected(),
        },
        "Created Pulsar cache reader",
      );
      logger.info("Create Pulsar consumer");
      consumer = await createPulsarConsumer(client, config.pulsar);
      logger.debug(
        {
          topicsPattern: config.pulsar.consumerConfig.topicsPattern,
          subscription: config.pulsar.consumerConfig.subscription,
          subscriptionType: config.pulsar.consumerConfig.subscriptionType,
          subscriptionInitialPosition:
            config.pulsar.consumerConfig.subscriptionInitialPosition,
          connected: consumer.isConnected(),
        },
        "Created Pulsar consumer",
      );
      logger.info("Set health check status to OK");
      setHealthOk(true);
      logger.info("Keep receiving, deduplicating and sending messages");
      await keepDeduplicating(
        logger,
        producer,
        cacheReader,
        consumer,
        config.cacheRebuild,
        config.deduplication,
      );
    } catch (err) {
      exitHandler(1, transformUnknownToError(err));
    }
  } catch (loggerErr) {
    // eslint-disable-next-line no-console
    console.error("Failed to start logging:", loggerErr);
    process.exit(1); // eslint-disable-line no-process-exit
  }
})();
