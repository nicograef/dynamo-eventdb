import { Stack, type StackProps } from 'aws-cdk-lib';
import { EventTable } from './EventTable.js';
import type { Construct } from 'constructs';
import type { IKey } from 'aws-cdk-lib/aws-kms';
import { Topic } from './Topic.js';
import { Lambda } from './Lambda.js';
import { fileURLToPath } from 'url';
import path from 'node:path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export type EventDBProps = {
  readonly isProdEnv: boolean;
  readonly tablename: string;
  readonly subscriptionAccounts: string[];
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

    const publishHandlerEntry = path.join(__dirname, '..', 'lambdas', 'publish.lambda.ts');
    this.publishLambda = new Lambda(this, 'PublishEvents', {
      isProdEnv: props.isProdEnv,
      handlerPath: publishHandlerEntry,
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
