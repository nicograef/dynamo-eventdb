import { Stack, type StackProps } from 'aws-cdk-lib';
import { EventTable } from './EventTable.js';
import type { Construct } from 'constructs';
import type { IKey } from 'aws-cdk-lib/aws-kms';
import { Topic } from './Topic.js';
import { Lambda } from './Lambda.js';

export type EventDBProps = {
  readonly isProdEnv: boolean;
  readonly tablename: string;
  readonly subscriptionAccounts: string[];
  readonly publishLambdaPath: string;
  readonly encryptionKey?: IKey;
} & StackProps;

export class EventDB extends Stack {
  public readonly table: EventTable;
  public readonly topic: Topic;
  private readonly publishLambda: Lambda;

  constructor(scope: Construct, id: string, props: EventDBProps) {
    super(scope, id, props);

    this.table = new EventTable(this, props.tablename, {
      isProdEnv: props.isProdEnv,
      encryptionKey: props.encryptionKey,
    });

    this.topic = new Topic(this, `${props.tablename}Topic`, {
      description: `Topic for ${props.tablename}`,
      subscriptionAccounts: props.subscriptionAccounts,
      encryptionKey: props.encryptionKey,
    });

    this.publishLambda = new Lambda(this, 'PublishEvents', {
      isProdEnv: props.isProdEnv,
      handlerPath: props.publishLambdaPath,
      description: 'Lambda to publish events to the event topic',
      encryptionKey: props.encryptionKey,
      environment: {
        EVENT_TOPIC_ARN: this.topic.topicArn,
      },
    });

    this.table.grantStreamRead(this.publishLambda.function);
    this.publishLambda.addDynamoEventSource(this.table.table);
    this.topic.grantPublish(this.publishLambda.function);
  }
}
