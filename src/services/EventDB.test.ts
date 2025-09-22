import { describe, expect, test, vi } from 'vitest';

import { EventDB } from './EventDB.js';
import { Cloudevent, type Event, type EventCandidate } from './Cloudevent.js';

const logger = { error: vi.fn() };

describe('addEvent', () => {
  test('add event into table with correct primary key', async () => {
    const dynamo = { send: vi.fn() };
    const { error, event } = Cloudevent.new({
      type: 'book.borrowed',
      subject: 'b111',
      source: 'https://library.example.com',
      data: { id: '111', title: 'Der Prozess' },
    });
    if (error) throw error;

    const table = new EventDB(logger, dynamo, 'BookEvents');
    await table.addEvents([event]);

    expect(dynamo.send).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          RequestItems: expect.objectContaining({
            BookEvents: [{ PutRequest: { Item: { ...event, time: event.time.getTime(), time_type: `${event.time.getTime()}_${event.type}`, pk_all: 'all' } } }],
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
      time: new Date(),
      data: 'not-an-object',
    };

    const table = new EventDB(logger, dynamo, 'BookEvents');
    await expect(() => table.addEvents([invalidEvent as unknown as Event])).rejects.toThrow('Failed to convert events to DynamoDB format');

    expect(dynamo.send).not.toHaveBeenCalled();
  });
});

describe('addNewEvent', () => {
  test('creates and adds event', async () => {
    const dynamo = { send: vi.fn() };
    const service = new EventDB(logger, dynamo, 'BookEvents');
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
      time: expect.any(Date),
    });
    expect(dynamo.send).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          RequestItems: expect.objectContaining({
            BookEvents: [
              {
                PutRequest: {
                  Item: { ...events[0], time: events[0]!.time.getTime(), time_type: `${events[0]!.time.getTime()}_${events[0]!.type}`, pk_all: 'all' },
                },
              },
            ],
          }),
        }),
      }),
    );
  });

  test('throws error when event is invalid', async () => {
    const dynamo = { send: vi.fn() };
    const service = new EventDB(logger, dynamo, 'BookEvents');

    const candidate: EventCandidate = {
      source: 'not-a-url',
      type: 'b',
      subject: '',
      data: { userId: 'u123' },
    };

    await expect(() => service.addNewEvents([candidate])).rejects.toThrow('Failed to create events');

    expect(dynamo.send).not.toHaveBeenCalled();
  });
});
