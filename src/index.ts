import pino from "pino";
import type Pulsar from "pulsar-client";
import { getConfig } from "./config.js";
import keepDeduplicating from "./deduplication.js";
import createHealthCheckServer from "./healthCheck.js";
import {
  createPulsarClient,
  createPulsarProducer,
  createPulsarConsumer,
} from "./pulsar.js";
import transformUnknownToError from "./util.js";

/**
 * Exit gracefully.
 */
const exitGracefully = async (
  logger: pino.Logger,
  exitCode: number,
  exitError?: Error,
  setHealthOk?: (isOk: boolean) => void,
  closeHealthCheckServer?: () => Promise<void>,
  pulsarClient?: Pulsar.Client,
  pulsarProducer?: Pulsar.Producer,
  pulsarConsumer?: Pulsar.Consumer
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
      "Something went wrong when setting health checks to fail"
    );
  }
  try {
    if (pulsarConsumer) {
      logger.info("Close Pulsar consumer");
      await pulsarConsumer.close();
    }
  } catch (err) {
    logger.error({ err }, "Something went wrong when closing Pulsar consumer");
  }
  try {
    if (pulsarProducer) {
      logger.info("Flush Pulsar producer");
      await pulsarProducer.flush();
    }
  } catch (err) {
    logger.error({ err }, "Something went wrong when flushing Pulsar producer");
  }
  try {
    if (pulsarProducer) {
      logger.info("Close Pulsar producer");
      await pulsarProducer.close();
    }
  } catch (err) {
    logger.error({ err }, "Something went wrong when closing Pulsar producer");
  }
  try {
    if (pulsarClient) {
      logger.info("Close Pulsar client");
      await pulsarClient.close();
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
      "Something went wrong when closing health check server"
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
  try {
    const logger = pino({
      name: "pulsar-topic-deduplicator",
      timestamp: pino.stdTimeFunctions.isoTime,
    });

    let setHealthOk: (isOk: boolean) => void;
    let closeHealthCheckServer: () => Promise<void>;
    let pulsarClient: Pulsar.Client;
    let pulsarProducer: Pulsar.Producer;
    let pulsarConsumer: Pulsar.Consumer;

    const exitHandler = (exitCode: number, exitError?: Error) => {
      // Exit next.
      /* eslint-disable @typescript-eslint/no-floating-promises */
      exitGracefully(
        logger,
        exitCode,
        exitError,
        setHealthOk,
        closeHealthCheckServer,
        pulsarClient,
        pulsarProducer,
        pulsarConsumer
      );
      /* eslint-enable @typescript-eslint/no-floating-promises */
    };

    try {
      // Handle different kinds of exits.
      process.on("beforeExit", () => exitHandler(1, new Error("beforeExit")));
      process.on("unhandledRejection", (reason) =>
        exitHandler(1, transformUnknownToError(reason))
      );
      process.on("uncaughtException", (err) => exitHandler(1, err));
      process.on("SIGINT", (signal) => exitHandler(130, new Error(signal)));
      process.on("SIGQUIT", (signal) => exitHandler(131, new Error(signal)));
      process.on("SIGTERM", (signal) => exitHandler(143, new Error(signal)));

      logger.info("Read configuration");
      const config = getConfig(logger);
      logger.info("Create health check server");
      ({ closeHealthCheckServer, setHealthOk } = createHealthCheckServer(
        config.healthCheck
      ));
      logger.info("Create Pulsar client");
      pulsarClient = createPulsarClient(config.pulsar);
      logger.info("Create Pulsar producer");
      pulsarProducer = await createPulsarProducer(pulsarClient, config.pulsar);
      logger.info("Create Pulsar consumer");
      pulsarConsumer = await createPulsarConsumer(pulsarClient, config.pulsar);
      logger.info("Set health check status to OK");
      setHealthOk(true);
      logger.info("Keep receiving, deduplicating and sending messages");
      await keepDeduplicating(
        pulsarProducer,
        pulsarConsumer,
        config.deduplication
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
