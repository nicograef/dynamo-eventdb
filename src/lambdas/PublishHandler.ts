import type { DynamoDBBatchItemFailure, DynamoDBBatchResponse, DynamoDBStreamEvent } from 'aws-lambda';
import { SNSClient } from '@aws-sdk/client-sns';
import z from 'zod';
import { Logger } from '@aws-lambda-powertools/logger';
import { Publisher } from './Publisher.js';

const EnvSchema = z.object({
  EVENT_TOPIC_ARN: z.string().min(1),
});

export class PublishHandler {
  private readonly publisher: Pick<Publisher, 'processRecord'>;
  private readonly logger: Pick<Logger, 'error'>;

  constructor(logger: Pick<Logger, 'error'>, publisher: Pick<Publisher, 'processRecord'>) {
    this.logger = logger;
    this.publisher = publisher;
  }

  public static create() {
    const env = EnvSchema.parse(process.env);
    const logger = new Logger();
    const sns = new SNSClient({});
    const publisher = new Publisher(logger, sns, { topicArn: env.EVENT_TOPIC_ARN });
    const handler = new PublishHandler(logger, publisher);
    return handler.handle.bind(handler);
  }

  public async handle(event: DynamoDBStreamEvent): Promise<DynamoDBBatchResponse> {
    const batchItemFailures: DynamoDBBatchItemFailure[] = [];

    for (const record of event.Records) {
      try {
        await this.publisher.processRecord(record);
      } catch (error) {
        this.logger.error('Error processing record', { error, record });

        if (record.dynamodb?.SequenceNumber) {
          batchItemFailures.push({ itemIdentifier: record.dynamodb?.SequenceNumber });
        }
      }
    }

    return { batchItemFailures };
  }
}
