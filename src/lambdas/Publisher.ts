import { PublishCommand, type SNSClient } from '@aws-sdk/client-sns';
import type { Logger } from '@aws-lambda-powertools/logger';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import type { DynamoDBRecord } from 'aws-lambda';
import { EventSchema, type Event } from '../services/types.js';
import type { AttributeValue } from '@aws-sdk/client-dynamodb';

export type PublisherConfig = {
  readonly topicArn: string;
};

export class Publisher {
  private readonly config: PublisherConfig;
  private readonly logger: Pick<Logger, 'info' | 'error'>;
  private readonly sns: Pick<SNSClient, 'send'>;

  constructor(logger: Pick<Logger, 'info' | 'error'>, sns: SNSClient, config: PublisherConfig) {
    this.config = config;
    this.logger = logger;
    this.sns = sns;
  }

  public async processRecord(record: DynamoDBRecord): Promise<void> {
    if (record.eventName !== 'INSERT') {
      // In an event-sourcing database, we only expect INSERT events.
      this.logger.error(`Unexpected event name ${record.eventName}`, { record });
      return;
    }

    if (!record.dynamodb?.NewImage) {
      // The stream is configured to provide NEW_IMAGE, so this should never happen.
      this.logger.error('No NewImage found in record', { record });
      return;
    }

    // we need to marshall the dynamodb record into a javascript object
    // unfortunately, there is a small type difference bewteen the `AttributeValue` definition in dynamodb and the one in the lambda event
    const newImage = unmarshall(record.dynamodb.NewImage as Record<string, AttributeValue>);
    const { error, data: event } = EventSchema.safeParse(newImage);
    if (error) {
      this.logger.error('Failed to parse event', { error: error.issues });
      return;
    }

    await this.publish(event);

    this.logger.info('Published event', { eventId: event.id, eventType: event.type });
  }

  private async publish(event: Event): Promise<void> {
    const publishCommand = new PublishCommand({
      TopicArn: this.config.topicArn,
      Message: JSON.stringify(event.data),
      MessageAttributes: Publisher.getAttributes(event),
    });

    await this.sns.send(publishCommand);
  }

  private static getAttributes(event: Event) {
    return {
      id: { DataType: 'String', StringValue: event.id },
      type: { DataType: 'String', StringValue: event.type },
      time: { DataType: 'String', StringValue: new Date(event.time).toISOString() },
      subject: { DataType: 'String', StringValue: event.subject },
      source: { DataType: 'String', StringValue: event.source },
    };
  }
}
