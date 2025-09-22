import { describe, expect, test, vi } from 'vitest';

import { EventDB } from './EventDB.js';
import { type Event, type EventCandidate } from './types.js';

describe('addEvent', () => {
  test('add event into table with correct primary key', async () => {
    const dynamo = { send: vi.fn() };
    const event: Event = {
      id: crypto.randomUUID(),
      type: 'book.borrowed',
      subject: 'b111',
      source: 'https://library.example.com',
      time: Date.now(),
      data: { id: '111', title: 'Der Prozess' },
    };

    const table = new EventDB(dynamo, 'BookEvents');
    await table.addEvents([event]);

    expect(dynamo.send).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          RequestItems: expect.objectContaining({
            BookEvents: [{ PutRequest: { Item: { ...event, time_type: `${event.time}_${event.type}`, pk_all: 'all' } } }],
          }),
        }),
      }),
    );
  });

  test('throws error when event is invalid', async () => {
    const dynamo = { send: vi.fn() };
    const invalidEvent = {
      id: '1', // too short
      type: 'a',
      subject: '',
      source: 'not-a-url',
      time: -123,
      data: 'not-an-object',
    };

    const table = new EventDB(dynamo, 'BookEvents');
    await expect(() => table.addEvents([invalidEvent as unknown as Event])).rejects.toThrow('Invalid event');

    expect(dynamo.send).not.toHaveBeenCalled();
  });
});

describe('addNewEvent', () => {
  test('creates and adds event', async () => {
    const dynamo = { send: vi.fn() };
    const service = new EventDB(dynamo, 'BookEvents');
    const candidate: EventCandidate = {
      source: 'https://library.example.com',
      type: 'book.borrowed',
      subject: 'b111',
      data: { userId: 'u123' },
    };

    const events = await service.addNewEvents([candidate]);

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      ...candidate,
      id: expect.any(String),
      time: expect.any(Number),
    });
    expect(dynamo.send).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          RequestItems: expect.objectContaining({
            BookEvents: [{ PutRequest: { Item: { ...events[0], time_type: `${events[0]!.time}_${events[0]!.type}`, pk_all: 'all' } } }],
          }),
        }),
      }),
    );
  });

  test('throws error when event is invalid', async () => {
    const dynamo = { send: vi.fn() };
    const service = new EventDB(dynamo, 'BookEvents');

    const candidate: EventCandidate = {
      source: 'not-a-url',
      type: 'b',
      subject: '',
      data: { userId: 'u123' },
    };

    await expect(() => service.addNewEvents([candidate])).rejects.toThrow('Invalid event');

    expect(dynamo.send).not.toHaveBeenCalled();
  });
});
