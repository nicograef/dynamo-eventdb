import { Stack, type StackProps, RemovalPolicy } from 'aws-cdk-lib';
import type { Construct } from 'constructs';
import { AttributeType, TableEncryptionV2, TableV2, StreamViewType, ProjectionType } from 'aws-cdk-lib/aws-dynamodb';
import type { IKey } from 'aws-cdk-lib/aws-kms';
import { PublishLambda } from './PublishLambda.js';
import { AccountPrincipal } from 'aws-cdk-lib/aws-iam';
import { Topic } from 'aws-cdk-lib/aws-sns';

export type EventDBProps = {
  readonly isProdEnv: boolean;
  readonly subscriptionAccounts: string[];
  readonly encryptionKey?: IKey;
} & StackProps;

export class EventDB extends Stack {
  public readonly eventTable: TableV2;
  public readonly eventTopic: Topic;
  public readonly publishLambda: PublishLambda;

  constructor(scope: Construct, id: string, props: EventDBProps) {
    super(scope, id, props);

    this.eventTable = new TableV2(this, `${id}DB`, {
      tableName: `${id}DB`,
      removalPolicy: props.isProdEnv ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      encryption: props.encryptionKey ? TableEncryptionV2.customerManagedKey(props.encryptionKey) : undefined,
      partitionKey: { name: 'subject', type: AttributeType.STRING },
      sortKey: { name: 'time_type', type: AttributeType.STRING },
      dynamoStream: StreamViewType.NEW_IMAGE, // Enable DynamoDB Streams so that Lambdas can subscribe to item changes.
      globalSecondaryIndexes: [
        {
          // For LIST queries that fetch events in time order, regardless of subject.
          indexName: 'AllEventsByTime',
          partitionKey: { name: 'pk_all', type: AttributeType.STRING },
          sortKey: { name: 'time', type: AttributeType.NUMBER },
          projectionType: ProjectionType.ALL,
        },
        {
          // For GET queries that fetch a specific event by its id.
          indexName: 'AllEventsById',
          partitionKey: { name: 'id', type: AttributeType.STRING },
          projectionType: ProjectionType.KEYS_ONLY,
        },
      ],
      localSecondaryIndexes: [
        {
          // For queries like "get all events of this type for this subject"
          indexName: 'SubjectEventsByType',
          sortKey: { name: 'type', type: AttributeType.STRING },
          projectionType: ProjectionType.ALL,
        },
      ],
    });

    this.eventTopic = new Topic(this, `${id}Topic`, {
      topicName: `${id}Topic`,
      displayName: `Topic for ${id}`,
      masterKey: props.encryptionKey,
      enforceSSL: true,
    });

    for (const id of props.subscriptionAccounts) {
      this.eventTopic.grantSubscribe(new AccountPrincipal(id));
    }

    this.publishLambda = new PublishLambda(this, {
      isProdEnv: props.isProdEnv,
      eventTable: this.eventTable,
      eventTopic: this.eventTopic,
      encryptionKey: props.encryptionKey,
    });
  }
}
