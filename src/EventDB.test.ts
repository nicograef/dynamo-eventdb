import { describe, expect, test, vi } from 'vitest';

import { EventDB } from './EventDB.js';
import { type Event } from './types.js';

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
    await table.addEvent(event);

    expect(dynamo.send).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          TableName: 'BookEvents',
          Item: event,
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
    await expect(() => table.addEvent(invalidEvent as unknown as Event)).rejects.toThrow('Invalid event');

    expect(dynamo.send).not.toHaveBeenCalled();
  });
});

describe('addNewEvent', () => {
  test('creates and adds event', async () => {
    const dynamo = { send: vi.fn() };
    const service = new EventDB(dynamo, 'BookEvents');

    const source = 'https://library.example.com';
    const type = 'book.borrowed';
    const subject = 'b111';
    const payload = { userId: 'u123' };
    const event = await service.addNewEvent({ source, type, subject, payload });

    expect(dynamo.send).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          TableName: 'BookEvents',
          Item: event,
        }),
      }),
    );
  });

  test('throws error when event is invalid', async () => {
    const dynamo = { send: vi.fn() };
    const service = new EventDB(dynamo, 'BookEvents');

    const source = 'not-a-url';
    const type = 'b';
    const subject = '';
    const payload = { userId: 'u123' };

    await expect(() => service.addNewEvent({ source, type, subject, payload })).rejects.toThrow('Invalid event');

    expect(dynamo.send).not.toHaveBeenCalled();
  });
});
