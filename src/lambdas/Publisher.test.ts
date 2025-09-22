import { vi, describe, test, expect } from 'vitest';
import { Publisher } from './Publisher.js';
import type { DynamoDBRecord } from 'aws-lambda';

describe('processRecord', () => {
  test('logs error and returns for non-INSERT event', async () => {
    const logger = { info: vi.fn(), error: vi.fn() };
    const sns = { send: vi.fn() };
    const publisher = new Publisher(logger, sns, { topicArn: '' });

    const recordModify = { eventName: 'MODIFY', dynamodb: { NewImage: {} } };
    const recordRemove = { eventName: 'REMOVE', dynamodb: { NewImage: {} } };
    await publisher.processRecord(recordModify as DynamoDBRecord);
    await publisher.processRecord(recordRemove as DynamoDBRecord);

    expect(logger.error).toHaveBeenCalledTimes(2);
    expect(sns.send).not.toHaveBeenCalled();
  });

  test('logs error and returns if NewImage is missing', async () => {
    const logger = { info: vi.fn(), error: vi.fn() };
    const sns = { send: vi.fn() };
    const publisher = new Publisher(logger, sns, { topicArn: '' });

    const record = { eventName: 'INSERT', dynamodb: {} };
    await publisher.processRecord(record as DynamoDBRecord);

    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(sns.send).not.toHaveBeenCalled();
  });

  test('logs error and returns if event parsing fails', async () => {
    const logger = { info: vi.fn(), error: vi.fn() };
    const sns = { send: vi.fn() };
    const publisher = new Publisher(logger, sns, { topicArn: '' });

    const record = {
      eventName: 'INSERT',
      dynamodb: {
        NewImage: {
          id: { S: '1' },
          time: { N: '1696543200000' },
          time_type: { S: '1696543200000_book.borrowed' },
          pk_all: { S: 'all' },
          type: { S: 'book.borrowed' },
          source: { S: 'https://library.example.com' },
          data: { M: { userId: { S: 'u123' } } },
        },
      },
    };
    await publisher.processRecord(record as DynamoDBRecord);

    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(sns.send).not.toHaveBeenCalled();
  });

  test('publishes event and logs info on success', async () => {
    const logger = { info: vi.fn(), error: vi.fn() };
    const sns = { send: vi.fn() };
    const publisher = new Publisher(logger, sns, { topicArn: 'arn:aws:sns:us-east-1:123456789012:MyTopic' });

    const record = {
      eventName: 'INSERT',
      dynamodb: {
        NewImage: {
          id: { S: 'e123' },
          time: { N: '1696543200000' },
          time_type: { S: '1696543200000_book.borrowed' },
          pk_all: { S: 'all' },
          type: { S: 'book.borrowed' },
          subject: { S: 'b111' },
          source: { S: 'https://library.example.com' },
          data: { M: { userId: { S: 'u123' } } },
        },
      },
    };
    await publisher.processRecord(record as DynamoDBRecord);

    expect(logger.error).not.toHaveBeenCalled();
    expect(sns.send).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          TopicArn: 'arn:aws:sns:us-east-1:123456789012:MyTopic',
          Message: JSON.stringify({
            id: 'e123',
            type: 'book.borrowed',
            subject: 'b111',
            source: 'https://library.example.com',
            time: new Date(1696543200000).toISOString(),
            data: { userId: 'u123' },
          }),
          MessageAttributes: expect.objectContaining({
            id: { DataType: 'String', StringValue: 'e123' },
            type: { DataType: 'String', StringValue: 'book.borrowed' },
            time: { DataType: 'String', StringValue: new Date(1696543200000).toISOString() },
          }),
        }),
      }),
    );
  });
});
