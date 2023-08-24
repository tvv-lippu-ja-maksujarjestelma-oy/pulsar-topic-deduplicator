import crypto from "node:crypto";
import { ObliviousSet } from "oblivious-set";
import Pulsar from "pulsar-client";
import { createHasher } from "./deduplication";

const mockPulsarMessage = ({
  properties,
  data,
  eventTimestamp,
}: {
  properties: { [key: string]: string };
  data: Buffer;
  eventTimestamp: number;
}): Pulsar.Message => {
  const message = Object.defineProperties(new Pulsar.Message(), {
    getData: {
      value: () => data,
      writable: true,
    },
    getEventTimestamp: {
      value: () => eventTimestamp,
      writable: true,
    },
    getProperties: {
      value: () => properties,
      writable: true,
    },
  });
  return message;
};

describe("calculateHash", () => {
  const expectEqualHashes = (
    calculateHash: (message: Pulsar.Message) => Uint8Array,
    message1: Pulsar.Message,
    message2: Pulsar.Message,
  ): void => {
    const hash1 = calculateHash(message1);
    const hash2 = calculateHash(message2);
    expect(hash1).toStrictEqual(hash2);
  };

  const expectDifferingHashes = (
    calculateHash: (message: Pulsar.Message) => Uint8Array,
    message1: Pulsar.Message,
    message2: Pulsar.Message,
  ): void => {
    const hash1 = calculateHash(message1);
    const hash2 = calculateHash(message2);
    expect(hash1).not.toStrictEqual(hash2);
  };

  describe("Synthetic, simple examples", () => {
    const messageData1 = Buffer.from("foo");
    const messageData2 = Buffer.from("bar");
    const noProperties = {};
    const properties1 = { baz: "qux" };
    const properties2 = { plugh: "xyzzy" };
    const propertiesToIgnore = { corge: "grault" };
    const eventTimestamp1 = 1;
    const eventTimestamp2 = 2;

    // eslint-disable-next-line jest/expect-expect
    test("Produce identical hashes for two messages with identical data, no properties and identical event timestamps", () => {
      const calculateHash = createHasher([]);
      const message1 = mockPulsarMessage({
        properties: noProperties,
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      const message2 = mockPulsarMessage({
        properties: noProperties,
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      expectEqualHashes(calculateHash, message1, message2);
    });

    // Use a shared function for assertions.
    // eslint-disable-next-line jest/expect-expect
    test("Produce identical hashes for two messages with identical data, no properties and differing event timestamps", () => {
      const calculateHash = createHasher([]);
      const message1 = mockPulsarMessage({
        properties: noProperties,
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      const message2 = mockPulsarMessage({
        properties: noProperties,
        data: messageData1,
        eventTimestamp: eventTimestamp2,
      });
      expectEqualHashes(calculateHash, message1, message2);
    });

    // Use a shared function for assertions.
    // eslint-disable-next-line jest/expect-expect
    test("Produce identical hashes for two messages with identical data, identical properties and differing event timestamps", () => {
      const calculateHash = createHasher([]);
      const message1 = mockPulsarMessage({
        properties: properties1,
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      const message2 = mockPulsarMessage({
        properties: properties1,
        data: messageData1,
        eventTimestamp: eventTimestamp2,
      });
      expectEqualHashes(calculateHash, message1, message2);
    });

    // Use a shared function for assertions.
    // eslint-disable-next-line jest/expect-expect
    test("Produce identical hashes for two messages with identical data, identical non-ignored properties and differing event timestamps", () => {
      const calculateHash = createHasher(Object.keys(propertiesToIgnore));
      const message1 = mockPulsarMessage({
        properties: { ...properties1, ...propertiesToIgnore },
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      const message2 = mockPulsarMessage({
        properties: { ...properties1, ...propertiesToIgnore },
        data: messageData1,
        eventTimestamp: eventTimestamp2,
      });
      expectEqualHashes(calculateHash, message1, message2);
    });

    // Use a shared function for assertions.
    // eslint-disable-next-line jest/expect-expect
    test("Produce differing hashes for two messages with identical data, differing properties and identical event timestamps", () => {
      const calculateHash = createHasher([]);
      const message1 = mockPulsarMessage({
        properties: properties1,
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      const message2 = mockPulsarMessage({
        properties: properties2,
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      expectDifferingHashes(calculateHash, message1, message2);
    });

    // Use a shared function for assertions.
    // eslint-disable-next-line jest/expect-expect
    test("Produce differing hashes for two messages with identical data, differing non-ignored properties and identical event timestamps", () => {
      const calculateHash = createHasher(Object.keys(propertiesToIgnore));
      const message1 = mockPulsarMessage({
        properties: { ...properties1, ...propertiesToIgnore },
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      const message2 = mockPulsarMessage({
        properties: { ...properties2, ...propertiesToIgnore },
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      expectDifferingHashes(calculateHash, message1, message2);
    });

    // Use a shared function for assertions.
    // eslint-disable-next-line jest/expect-expect
    test("Produce differing hashes for two messages with differing data, identical properties and identical event timestamps", () => {
      const calculateHash = createHasher([]);
      const message1 = mockPulsarMessage({
        properties: properties1,
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      const message2 = mockPulsarMessage({
        properties: properties1,
        data: messageData2,
        eventTimestamp: eventTimestamp1,
      });
      expectDifferingHashes(calculateHash, message1, message2);
    });

    test("Produce identical hashes for identical input with two separate hasher instances", () => {
      const calculateHash1 = createHasher([]);
      const calculateHash2 = createHasher([]);
      const message = mockPulsarMessage({
        properties: properties1,
        data: messageData1,
        eventTimestamp: eventTimestamp1,
      });
      const hash1 = calculateHash1(message);
      const hash2 = calculateHash2(message);
      expect(hash1).toStrictEqual(hash2);
    });
  });

  describe("Realistic cases", () => {
    test("Realistic case", () => {
      const ignoredProperties = [
        "mqttQos",
        "mqttIsRetained",
        "mqttIsDuplicate",
      ];
      const calculateHash = createHasher(ignoredProperties);

      const content = {
        APC: {
          tst: "2023-04-06T09:09:29Z",
          lat: 62.384937,
          long: 25.678112,
          vehiclecounts: {
            vehicleload: 23,
            doorcounts: [
              { door: 1, count: [{ class: "adult", in: 3, out: 0 }] },
            ],
            countquality: "regular",
          },
          schemaVersion: "1-1-0",
          messageId: "eb7baf07-4f5c-463d-a91e-f501403c1a3f",
        },
      };
      const data1 = Buffer.from(JSON.stringify(content), "utf8");
      const properties1 = {
        mqttTopic: "apc-from-vehicle/v1/fi/waltti/telia/JL521-APC",
        mqttQos: "1",
        mqttIsRetained: "false",
        mqttIsDuplicate: "false",
      };
      const eventTimestamp1 = new Date("2023-04-06T09:09:34.351Z").getTime();
      const message1 = mockPulsarMessage({
        properties: properties1,
        data: data1,
        eventTimestamp: eventTimestamp1,
      });

      const data2 = Buffer.from(JSON.stringify(content), "utf8");
      const properties2 = { ...properties1 };
      const eventTimestamp2 = new Date("2023-04-06T09:09:34.321Z").getTime();
      const message2 = mockPulsarMessage({
        properties: properties2,
        data: data2,
        eventTimestamp: eventTimestamp2,
      });
      const hash1 = calculateHash(message1);
      const hash2 = calculateHash(message2);
      expect(hash1).toStrictEqual(hash2);
    });
  });
});

describe("cache", () => {
  test("cache can handle Buffers", () => {
    const generateRandomData = (nBits: number): Uint8Array => {
      const byteArray = new Uint8Array(nBits);
      return crypto.getRandomValues(byteArray);
    };

    const cache = new ObliviousSet(100 * 1e3);
    const buffer = Buffer.from(generateRandomData(512));
    cache.add(buffer);
    expect(cache.has(buffer)).toBeTruthy();
  });
});
