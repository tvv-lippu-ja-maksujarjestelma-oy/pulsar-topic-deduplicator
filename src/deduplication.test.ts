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

const expectEqualHashes = (
  calculateHash: (message: Pulsar.Message) => Uint8Array,
  message1: Pulsar.Message,
  message2: Pulsar.Message
): void => {
  const hash1 = calculateHash(message1);
  const hash2 = calculateHash(message2);
  expect(hash1).toStrictEqual(hash2);
};

const expectDifferingHashes = (
  calculateHash: (message: Pulsar.Message) => Uint8Array,
  message1: Pulsar.Message,
  message2: Pulsar.Message
): void => {
  const hash1 = calculateHash(message1);
  const hash2 = calculateHash(message2);
  expect(hash1).not.toStrictEqual(hash2);
};

const messageData1 = Buffer.from("foo");
const messageData2 = Buffer.from("bar");
const noProperties = {};
const properties1 = { baz: "qux" };
const properties2 = { plugh: "xyzzy" };
const propertiesToIgnore = { corge: "grault" };
const eventTimestamp1 = 1;
const eventTimestamp2 = 2;

// eslint-disable-next-line jest/expect-expect
test("Produce identical hashes for two messages with identical data, no properties and identical event timestamps", async () => {
  const calculateHash = await createHasher([]);
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
test("Produce identical hashes for two messages with identical data, no properties and differing event timestamps", async () => {
  const calculateHash = await createHasher([]);
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
test("Produce identical hashes for two messages with identical data, identical properties and differing event timestamps", async () => {
  const calculateHash = await createHasher([]);
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
test("Produce identical hashes for two messages with identical data, identical non-ignored properties and differing event timestamps", async () => {
  const calculateHash = await createHasher(Object.keys(propertiesToIgnore));
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
test("Produce differing hashes for two messages with identical data, differing properties and identical event timestamps", async () => {
  const calculateHash = await createHasher([]);
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
test("Produce differing hashes for two messages with identical data, differing non-ignored properties and identical event timestamps", async () => {
  const calculateHash = await createHasher(Object.keys(propertiesToIgnore));
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
test("Produce differing hashes for two messages with differing data, identical properties and identical event timestamps", async () => {
  const calculateHash = await createHasher([]);
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

test("Produce identical hashes for identical input with two separate hasher instances", async () => {
  const calculateHash1 = await createHasher([]);
  const calculateHash2 = await createHasher([]);
  const message = mockPulsarMessage({
    properties: properties1,
    data: messageData1,
    eventTimestamp: eventTimestamp1,
  });
  const hash1 = calculateHash1(message);
  const hash2 = calculateHash2(message);
  expect(hash1).toStrictEqual(hash2);
});
