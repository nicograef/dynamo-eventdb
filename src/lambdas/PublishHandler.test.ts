import { vi, describe, test, expect } from 'vitest';
import { PublishHandler } from './PublishHandler.js';
import type { DynamoDBRecord } from 'aws-lambda';

describe('handle', () => {
  test('should process records', async () => {
    const processRecord = vi.fn();
    const handler = new PublishHandler({ error: vi.fn() }, { processRecord });

    const event = { Records: [{} as DynamoDBRecord, {} as DynamoDBRecord] };

    const result = await handler.handle(event);

    expect(processRecord).toHaveBeenCalledTimes(2);
    expect(result).toEqual({ batchItemFailures: [] });
  });

  test('should report failed records', async () => {
    const processRecord = vi.fn().mockRejectedValueOnce(new Error('Failed'));
    const handler = new PublishHandler({ error: vi.fn() }, { processRecord });

    const event = { Records: [{ dynamodb: { SequenceNumber: '1' } } as DynamoDBRecord, { dynamodb: { SequenceNumber: '2' } } as DynamoDBRecord] };

    const result = await handler.handle(event);

    expect(processRecord).toHaveBeenCalledTimes(2);
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: '1' }] });
  });
});

describe('create', () => {
  test('should throw if env is missing', () => {
    vi.stubEnv('EVENT_TOPIC_ARN', '');
    expect(() => PublishHandler.create()).toThrow();
  });

  test('should create handler if env is set', () => {
    vi.stubEnv('EVENT_TOPIC_ARN', 'arn:aws:sns:region:account-id:topic-name');
    expect(typeof PublishHandler.create()).toBe('function');
  });
});
