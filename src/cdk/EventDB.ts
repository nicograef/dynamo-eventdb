import { Stack, type StackProps } from 'aws-cdk-lib';
import { EventTable } from './EventTable.js';
import type { Construct } from 'constructs';
import type { IKey } from 'aws-cdk-lib/aws-kms';
import { PublishLambda } from './PublishLambda.js';
import { AccountPrincipal } from 'aws-cdk-lib/aws-iam';
import { Topic } from 'aws-cdk-lib/aws-sns';

export type EventDBProps = {
  readonly isProdEnv: boolean;
  readonly tablename: string;
  readonly subscriptionAccounts: string[];
  readonly encryptionKey?: IKey;
} & StackProps;

export class EventDB extends Stack {
  public readonly eventTable: EventTable;
  public readonly eventTopic: Topic;
  public readonly publishLambda: PublishLambda;

  constructor(scope: Construct, id: string, props: EventDBProps) {
    super(scope, id, props);

    this.eventTable = new EventTable(this, props.tablename, {
      isProdEnv: props.isProdEnv,
      encryptionKey: props.encryptionKey,
    });

    this.eventTopic = new Topic(this, `${props.tablename}Topic`, {
      topicName: id,
      displayName: `Topic for ${props.tablename}`,
      masterKey: props.encryptionKey,
      enforceSSL: true,
    });

    for (const id of props.subscriptionAccounts) {
      this.eventTopic.grantSubscribe(new AccountPrincipal(id));
    }

    this.publishLambda = new PublishLambda(this, {
      isProdEnv: props.isProdEnv,
      eventTable: this.eventTable.table,
      eventTopic: this.eventTopic,
      encryptionKey: props.encryptionKey,
    });
  }
}
