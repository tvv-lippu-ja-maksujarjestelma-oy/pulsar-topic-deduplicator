import type pino from "pino";
import Pulsar from "pulsar-client";

export interface DeduplicationConfig {
  deduplicationWindowInSeconds: number;
  ignoredProperties: string[];
}

export interface PulsarOauth2Config {
  // pulsar-client requires "type" but that seems unnecessary
  type: string;
  issuer_url: string;
  client_id?: string;
  client_secret?: string;
  private_key?: string;
  audience?: string;
  scope?: string;
}

export interface PulsarConfig {
  oauth2Config: PulsarOauth2Config;
  clientConfig: Pulsar.ClientConfig;
  producerConfig: Pulsar.ProducerConfig;
  consumerConfig: Pulsar.ConsumerConfig;
}

export interface HealthCheckConfig {
  port: number;
}

export interface Config {
  deduplication: DeduplicationConfig;
  pulsar: PulsarConfig;
  healthCheck: HealthCheckConfig;
}

const getRequired = (envVariable: string) => {
  const variable = process.env[envVariable];
  if (variable === undefined) {
    throw new Error(`${envVariable} must be defined`);
  }
  return variable;
};

const getOptional = (envVariable: string) => process.env[envVariable];

const getOptionalBooleanWithDefault = (
  envVariable: string,
  defaultValue: boolean,
) => {
  let result = defaultValue;
  const str = getOptional(envVariable);
  if (str !== undefined) {
    if (!["false", "true"].includes(str)) {
      throw new Error(`${envVariable} must be either "false" or "true"`);
    }
    result = str === "true";
  }
  return result;
};

const getOptionalFloat = (envVariable: string): number | undefined => {
  const string = getOptional(envVariable);
  return string !== undefined ? parseFloat(string) : undefined;
};

const getOptionalNonNegativeFloat = (
  envVariable: string,
): number | undefined => {
  const float = getOptionalFloat(envVariable);
  if (float != null && (!Number.isFinite(float) || float < 0)) {
    throw new Error(
      `${envVariable} must be a non-negative, finite float if given. Instead, ${float} was given.`,
    );
  }
  return float;
};

const getDeduplicationIgnoredProperties = (): string[] => {
  const envVariable = "DEDUPLICATION_IGNORED_PROPERTIES";
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
  const ignoredProperties: string[] = JSON.parse(
    getOptional(envVariable) ?? "[]",
  );
  if (
    !(
      Array.isArray(ignoredProperties) &&
      ignoredProperties.every((s: string) => typeof s === "string")
    )
  ) {
    throw new Error(
      `${envVariable} must be a stringified JSON array holding only strings.`,
    );
  }
  return ignoredProperties;
};

const getDeduplicationConfig = () => {
  const deduplicationWindowInSeconds =
    getOptionalNonNegativeFloat("DEDUPLICATION_WINDOW_IN_SECONDS") ?? 3600;
  const ignoredProperties = getDeduplicationIgnoredProperties();
  return {
    deduplicationWindowInSeconds,
    ignoredProperties,
  };
};

const getPulsarOauth2Config = () => ({
  // pulsar-client requires "type" but that seems unnecessary
  type: "client_credentials",
  issuer_url: getRequired("PULSAR_OAUTH2_ISSUER_URL"),
  private_key: getRequired("PULSAR_OAUTH2_KEY_PATH"),
  audience: getRequired("PULSAR_OAUTH2_AUDIENCE"),
});

const createPulsarLog =
  (logger: pino.Logger) =>
  (
    level: Pulsar.LogLevel,
    file: string,
    line: number,
    message: string,
  ): void => {
    switch (level) {
      case Pulsar.LogLevel.DEBUG:
        logger.debug({ file, line }, message);
        break;
      case Pulsar.LogLevel.INFO:
        logger.info({ file, line }, message);
        break;
      case Pulsar.LogLevel.WARN:
        logger.warn({ file, line }, message);
        break;
      case Pulsar.LogLevel.ERROR:
        logger.error({ file, line }, message);
        break;
      default: {
        const exhaustiveCheck: never = level;
        throw new Error(String(exhaustiveCheck));
      }
    }
  };

const getPulsarCompressionType = (): Pulsar.CompressionType => {
  const compressionType = getOptional("PULSAR_COMPRESSION_TYPE") ?? "ZSTD";
  // tsc does not understand:
  // if (!["Zlib", "LZ4", "ZSTD", "SNAPPY"].includes(compressionType)) {
  if (
    compressionType !== "Zlib" &&
    compressionType !== "LZ4" &&
    compressionType !== "ZSTD" &&
    compressionType !== "SNAPPY"
  ) {
    throw new Error(
      "If defined, PULSAR_COMPRESSION_TYPE must be one of 'Zlib', 'LZ4', " +
        "'ZSTD' or 'SNAPPY'. Default is 'ZSTD'.",
    );
  }
  return compressionType;
};

const getPulsarConfig = (logger: pino.Logger): PulsarConfig => {
  const oauth2Config = getPulsarOauth2Config();
  const serviceUrl = getRequired("PULSAR_SERVICE_URL");
  const tlsValidateHostname = getOptionalBooleanWithDefault(
    "PULSAR_TLS_VALIDATE_HOSTNAME",
    true,
  );
  const log = createPulsarLog(logger);
  const producerTopic = getRequired("PULSAR_PRODUCER_TOPIC");
  const blockIfQueueFull = getOptionalBooleanWithDefault(
    "PULSAR_BLOCK_IF_QUEUE_FULL",
    true,
  );
  const compressionType = getPulsarCompressionType();
  const consumerTopicsPattern = getRequired("PULSAR_CONSUMER_TOPICS_PATTERN");
  const subscription = getRequired("PULSAR_SUBSCRIPTION");
  const subscriptionType = "Exclusive";
  const subscriptionInitialPosition = "Earliest";
  return {
    oauth2Config,
    clientConfig: {
      serviceUrl,
      tlsValidateHostname,
      log,
    },
    producerConfig: {
      topic: producerTopic,
      blockIfQueueFull,
      compressionType,
    },
    consumerConfig: {
      topicsPattern: consumerTopicsPattern,
      subscription,
      subscriptionType,
      subscriptionInitialPosition,
    },
  };
};

const getHealthCheckConfig = () => {
  const port = parseInt(getOptional("HEALTH_CHECK_PORT") ?? "8080", 10);
  return { port };
};

export const getConfig = (logger: pino.Logger): Config => ({
  deduplication: getDeduplicationConfig(),
  pulsar: getPulsarConfig(logger),
  healthCheck: getHealthCheckConfig(),
});
