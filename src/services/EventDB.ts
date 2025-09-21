import { BillingMode, CreateTableCommand, KeyType, ProjectionType, ScalarAttributeType, type DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, QueryCommand, type QueryCommandInput } from '@aws-sdk/lib-dynamodb';
import { EventSchema, type Event } from './types.js';

export class EventDB {
  private readonly dynamodb: Pick<DynamoDBDocumentClient, 'send'>;
  private readonly name: string;

  constructor(dynamodb: Pick<DynamoDBDocumentClient, 'send'>, name: string) {
    this.dynamodb = dynamodb;
    this.name = name;
  }

  public static instance(dynamodbClient: DynamoDBClient, name: string): EventDB {
    const documentClient = DynamoDBDocumentClient.from(dynamodbClient, {
      marshallOptions: {
        convertEmptyValues: false,
        removeUndefinedValues: true,
        convertClassInstanceToMap: false,
      },
      unmarshallOptions: { wrapNumbers: false },
    });
    return new EventDB(documentClient, name);
  }

  /**
   * Creates a DynamoDB table in the required form for the EventDB.
   * The event's `subject` is used as the partition key because it allows efficient querying of all events for a specific subject.
   *
   * Using a composite sort key of `time` and `type` allows multiple events for the same subject
   * to appear at the same time without colliding (i.e. they would overwrite each other since they have the same primary key = partition key + sort key).
   * This also allows querying events for a specific subject in chronological order, since the sort key starts with the time.
   *
   * Additionally, we create one global secondary index:
   * - `AllEventsByTime`: Allows querying all events in the database sorted by time.
   */
  public static async createTable(dynamodbClient: DynamoDBClient, tableName: string): Promise<void> {
    await dynamodbClient.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [
          { AttributeName: 'subject', KeyType: KeyType.HASH },
          { AttributeName: 'time_type', KeyType: KeyType.RANGE },
        ],
        AttributeDefinitions: [
          { AttributeName: 'subject', AttributeType: ScalarAttributeType.S },
          { AttributeName: 'time_type', AttributeType: ScalarAttributeType.S },
          { AttributeName: 'pk_all', AttributeType: ScalarAttributeType.S },
          { AttributeName: 'time', AttributeType: ScalarAttributeType.N },
        ],
        GlobalSecondaryIndexes: [
          {
            IndexName: 'AllEventsByTime',
            KeySchema: [
              { AttributeName: 'pk_all', KeyType: KeyType.HASH },
              { AttributeName: 'time', KeyType: KeyType.RANGE },
            ],
            Projection: { ProjectionType: ProjectionType.ALL },
          },
        ],
        BillingMode: BillingMode.PAY_PER_REQUEST,
      }),
    );
  }

  /** Creates a new event with the given attributes (source, type, subject, payload) and adds it to the database. */
  public async addNewEvent({
    source,
    type,
    subject,
    payload,
  }: {
    source: string;
    type: string;
    subject: string;
    payload: Record<string, unknown>;
  }): Promise<Event> {
    const event: Event = {
      id: crypto.randomUUID(),
      source,
      type,
      subject,
      time: Date.now(),
      data: payload,
    };

    await this.addEvent(event);

    return event;
  }

  /** Adds the given event to the DynamoDB table. */
  public async addEvent(event: Event): Promise<void> {
    const { error: validationError, data: parsedEvent } = EventSchema.safeParse(event);
    if (validationError) {
      throw new Error(`Invalid event: ${validationError.message}`);
    }

    await this.dynamodb.send(
      new PutCommand({
        TableName: this.name,
        Item: {
          ...parsedEvent,
          time_type: `${parsedEvent.time}_${parsedEvent.type}`,
          pk_all: 'all',
        },
      }),
    );
  }

  /** Returns all events sorted by time. */
  public async fetchAllEvents(): Promise<{ validItems: Event[]; invalidItems: Record<string, unknown>[] }> {
    const { validItems, invalidItems } = await this.paginatedQuery({
      IndexName: 'AllEventsByTime',
      KeyConditionExpression: 'pk_all = :all',
      ExpressionAttributeValues: { ':all': 'all' },
      ScanIndexForward: true, // ascending order by time
    });

    return { validItems, invalidItems };
  }

  /** Returns all events for the given subject in chronological order. */
  public async fetchEventsForSubject(subject: string): Promise<{ validItems: Event[]; invalidItems: Record<string, unknown>[] }> {
    const { validItems, invalidItems } = await this.paginatedQuery({
      KeyConditionExpression: 'subject = :subject',
      ExpressionAttributeValues: { ':subject': subject },
      ScanIndexForward: true, // ascending order by time
    });

    return { validItems, invalidItems };
  }

  /** Takes a DynamoDB Query-Command and paginates through the table until DynamoDB stops returning results. */
  private async paginatedQuery(query: Partial<QueryCommandInput>): Promise<{ validItems: Event[]; invalidItems: Record<string, unknown>[] }> {
    const validItems: Event[] = [];
    const invalidItems: Record<string, unknown>[] = [];

    let lastEvaluatedKey: Record<string, unknown> | undefined;

    do {
      const result = await this.dynamodb.send(
        new QueryCommand({
          ...query,
          TableName: this.name,
          Limit: 100,
          ExclusiveStartKey: lastEvaluatedKey,
        }),
      );

      if (result.Items) {
        for (const item of result.Items) {
          const { success, data } = EventSchema.safeParse(item);
          if (success) {
            validItems.push(data);
          } else {
            invalidItems.push(item);
          }
        }
      }

      lastEvaluatedKey = result.LastEvaluatedKey;
    } while (lastEvaluatedKey);

    return { validItems, invalidItems };
  }
}
