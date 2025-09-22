import { RemovalPolicy } from 'aws-cdk-lib';
import { AttributeType, TableEncryptionV2, TableV2, StreamViewType, ProjectionType } from 'aws-cdk-lib/aws-dynamodb';
import type { IGrantable } from 'aws-cdk-lib/aws-iam';
import type { IKey } from 'aws-cdk-lib/aws-kms';
import { Construct } from 'constructs';

export type EventTableProps = {
  readonly isProdEnv: boolean;
  readonly encryptionKey?: IKey;
};

/**
 * Creates a DynamoDB table in the required form for the EventDB.
 * The event's `subject` is used as the partition key because it allows efficient querying of all events for a specific subject.
 *
 * Using a composite sort key of `time` and `type` allows multiple events for the same subject
 * to appear at the same time without colliding (i.e. they would overwrite each other since they have the same primary key = partition key + sort key).
 * This also allows querying events for a specific subject in chronological order, since the sort key starts with the time.
 *
 * Additionally, we create two global secondary indexes:
 * - `AllEventsByTime`: Allows querying all events in the database sorted by time.
 * - `AllEventsById`: Allows querying a specific event by its ID.
 */
export class EventTable extends Construct {
  public readonly table: TableV2;

  constructor(scope: Construct, id: string, props: EventTableProps) {
    super(scope, id);

    this.table = new TableV2(this, id, {
      tableName: id,
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
          // For queries like "get all eventy of this type for this subject"
          indexName: 'SubjectEventsByType',
          sortKey: { name: 'type', type: AttributeType.STRING },
          projectionType: ProjectionType.ALL,
        },
      ],
    });
  }

  public get tableName(): string {
    return this.table.tableName;
  }

  public get tableArn(): string {
    return this.table.tableArn;
  }

  public grantReadData(grantee: IGrantable) {
    this.table.grantReadData(grantee);
  }

  public grantWriteData(grantee: IGrantable) {
    this.table.grantWriteData(grantee);
  }

  public grantReadWriteData(grantee: IGrantable) {
    this.table.grantReadWriteData(grantee);
  }

  public grantStreamRead(grantee: IGrantable) {
    this.table.grantStreamRead(grantee);
  }
}
