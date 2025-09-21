import type { DynamoDBBatchItemFailure, DynamoDBBatchResponse, DynamoDBStreamEvent } from 'aws-lambda';
import { SNSClient } from '@aws-sdk/client-sns';
import z from 'zod';
import { Logger } from '@aws-lambda-powertools/logger';
import { Publisher } from './Publisher.js';

const env = z
  .object({
    EVENT_TOPIC_ARN: z.string().min(1),
  })
  .parse(process.env);

const logger = new Logger();
const sns = new SNSClient({});

export class Handler {
  private readonly publisher: Publisher;

  constructor(publisher: Publisher) {
    this.publisher = publisher;
  }

  public static create() {
    const publisher = new Publisher(logger, sns, { topicArn: env.EVENT_TOPIC_ARN });
    const handler = new Handler(publisher);
    return handler.handle.bind(handler);
  }

  public async handle(event: DynamoDBStreamEvent): Promise<DynamoDBBatchResponse> {
    const batchItemFailures: DynamoDBBatchItemFailure[] = [];

    for (const record of event.Records) {
      try {
        await this.publisher.processRecord(record);
      } catch (error) {
        logger.error('Error processing record', { error, record });

        if (record.dynamodb?.SequenceNumber) {
          batchItemFailures.push({ itemIdentifier: record.dynamodb?.SequenceNumber });
        }
      }
    }

    return { batchItemFailures };
  }
}

export const handler = Handler.create();
