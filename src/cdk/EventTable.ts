import { RemovalPolicy } from 'aws-cdk-lib';
import { AttributeType, TableEncryptionV2, TableV2, StreamViewType } from 'aws-cdk-lib/aws-dynamodb';
import type { IGrantable } from 'aws-cdk-lib/aws-iam';
import type { IKey } from 'aws-cdk-lib/aws-kms';
import { Construct } from 'constructs';

export type EventTableProps = {
  readonly isProdEnv: boolean;
  readonly encryptionKey?: IKey;
};

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
          indexName: 'AllEventsByTime',
          partitionKey: { name: 'pk_all', type: AttributeType.STRING },
          sortKey: { name: 'time', type: AttributeType.STRING },
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
