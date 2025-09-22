import { DeleteTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import { EventDB } from './EventDB.js';
import { Cloudevent } from './Cloudevent.js';

vi.stubEnv('AWS_ACCESS_KEY_ID', 'dummy');
vi.stubEnv('AWS_SECRET_ACCESS_KEY', 'dummy');
vi.stubEnv('AWS_REGION', 'eu-central-1');

const dynamo = new DynamoDBClient({ endpoint: 'http://localhost:8000' });
const tableName = 'BookEvents';

beforeEach(async () => {
  await EventDB.createTable(dynamo, tableName);
});

afterEach(async () => {
  await dynamo.send(new DeleteTableCommand({ TableName: tableName }));
});

test('add events', async () => {
  const table = EventDB.instance(console, dynamo, tableName);

  const { event: event1 } = Cloudevent.new({
    type: 'book.borrowed',
    subject: 'b111',
    source: 'https://library.example.com',
    data: { userId: 'u123' },
  });
  const { event: event2 } = Cloudevent.new({
    type: 'book.returned',
    subject: 'b222',
    source: 'https://library.example.com',
    data: {},
  });
  if (!event1 || !event2) throw new Error('Failed to create events');

  await table.addEvents([event1, event2]);
});

test('fetch all events', async () => {
  const table = EventDB.instance(console, dynamo, tableName);

  const { event: event1 } = Cloudevent.new({
    type: 'book.borrowed',
    subject: 'b111',
    source: 'https://library.example.com',
    data: { userId: 'u123' },
  });
  await new Promise((r) => setTimeout(r, 10)); // ensure different timestamps
  const { event: event2 } = Cloudevent.new({
    type: 'book.returned',
    subject: 'b222',
    source: 'https://library.example.com',
    data: {},
  });
  await new Promise((r) => setTimeout(r, 10)); // ensure different timestamps
  const { event: event3 } = Cloudevent.new({
    type: 'book.returned',
    subject: 'b111',
    source: 'https://library.example.com',
    data: {},
  });
  if (!event1 || !event2 || !event3) throw new Error('Failed to create events');

  await table.addEvents([event1, event2, event3]);
  const { validItems, invalidItems } = await table.fetchAllEvents();

  expect(invalidItems).toHaveLength(0);
  expect(validItems).toEqual([event1, event2, event3]);
});

test('fetch events for subject', async () => {
  const table = EventDB.instance(console, dynamo, tableName);

  const { event: event1 } = Cloudevent.new({
    type: 'book.borrowed',
    subject: 'b111',
    source: 'https://library.example.com',
    data: { userId: 'u123' },
  });
  await new Promise((r) => setTimeout(r, 10)); // ensure different timestamps
  const { event: event2 } = Cloudevent.new({
    type: 'book.returned',
    subject: 'b222',
    source: 'https://library.example.com',
    data: {},
  });
  await new Promise((r) => setTimeout(r, 10)); // ensure different timestamps
  const { event: event3 } = Cloudevent.new({
    type: 'book.returned',
    subject: 'b111',
    source: 'https://library.example.com',
    data: {},
  });
  if (!event1 || !event2 || !event3) throw new Error('Failed to create events');

  await table.addEvents([event1, event2, event3]);
  const { validItems, invalidItems } = await table.fetchEventsForSubject('b111');

  expect(invalidItems).toHaveLength(0);
  expect(validItems).toEqual([event1, event3]);
});
