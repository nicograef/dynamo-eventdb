import { describe, expect, test } from 'vitest';
import { Cloudevent, type DynamoEvent } from './Cloudevent.js';

describe('new', () => {
  test('creates a new event with valid data', () => {
    const { error, event } = Cloudevent.new({
      type: 'book.borrowed',
      subject: 'b111',
      source: 'https://library.example.com',
      data: { userId: 'u123' },
    });

    expect(error).toBeUndefined();
    expect(event).toStrictEqual({
      id: expect.any(String),
      time: expect.any(Date),
      type: 'book.borrowed',
      subject: 'b111',
      source: 'https://library.example.com',
      data: { userId: 'u123' },
    });
  });

  test('returns error with invalid data', () => {
    const { error, event } = Cloudevent.new({
      type: 'b',
      subject: 'b111',
      source: 'https://library.example.com',
      data: { userId: 'u123' },
    });

    expect(event).toBeUndefined();
    expect(error).toBeInstanceOf(Error);
  });
});

describe('fromDynamo', () => {
  test('creates an event from a valid DynamoEvent', () => {
    const { error, event } = Cloudevent.fromDynamo({
      id: 'e123',
      time: 1696543200000,
      time_type: '1696543200000_book.borrowed',
      pk_all: 'all',
      type: 'book.borrowed',
      subject: 'b111',
      source: 'https://library.example.com',
      data: { userId: 'u123' },
    });

    expect(error).toBeUndefined();
    expect(event).toStrictEqual({
      id: 'e123',
      time: new Date(1696543200000),
      type: 'book.borrowed',
      subject: 'b111',
      source: 'https://library.example.com',
      data: { userId: 'u123' },
    });
  });

  test('returns error with invalid dynamo data', () => {
    const { error, event } = Cloudevent.fromDynamo({
      id: 'e123',
      time: 1696543200000,
      time_type: '',
    } as DynamoEvent);

    expect(event).toBeUndefined();
    expect(error).toBeInstanceOf(Error);
  });

  test('returns error with invalid event data', () => {
    const { error, event } = Cloudevent.fromDynamo({
      id: 'e123',
      time: -1,
      time_type: '1696543200000_book.borrowed',
      pk_all: 'all',
      type: 'book.borrowed',
      subject: '',
      source: 'https://library.example.com',
      data: { userId: 'u123' },
    });

    expect(event).toBeUndefined();
    expect(error).toBeInstanceOf(Error);
  });
});

describe('toDynamo', () => {
  test('converts an Event to DynamoEvent', () => {
    const { error, dynamoEvent } = Cloudevent.toDynamo({
      id: 'e123',
      time: new Date(1696543200000),
      type: 'book.borrowed',
      subject: 'b111',
      source: 'https://library.example.com',
      data: { userId: 'u123' },
    });

    expect(error).toBeUndefined();
    expect(dynamoEvent).toStrictEqual({
      id: 'e123',
      time: 1696543200000,
      time_type: '1696543200000_book.borrowed',
      pk_all: 'all',
      type: 'book.borrowed',
      subject: 'b111',
      source: 'https://library.example.com',
      data: { userId: 'u123' },
    });
  });

  test('returns error with invalid Event', () => {
    const { error, dynamoEvent } = Cloudevent.toDynamo({
      id: '',
      time: new Date(1696543200000),
      type: 'book.borrowed',
      subject: '',
      source: 'https://library.example.com',
      data: { userId: 'u123' },
    });

    expect(dynamoEvent).toBeUndefined();
    expect(error).toBeInstanceOf(Error);
  });
});
