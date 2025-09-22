import { CreateTableCommand, KeyType, ProjectionType, ScalarAttributeType, type DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { BatchWriteCommand, DynamoDBDocumentClient, QueryCommand, type QueryCommandInput } from '@aws-sdk/lib-dynamodb';
import { Cloudevent, type EventCandidate, type Event, type DynamoEvent } from './Cloudevent.js';
import type { Logger } from '@aws-lambda-powertools/logger';

export class EventDB {
  private readonly logger: Pick<Logger, 'error'>;
  private readonly dynamodb: Pick<DynamoDBDocumentClient, 'send'>;
  private readonly name: string;

  constructor(logger: Pick<Logger, 'error'>, dynamodb: Pick<DynamoDBDocumentClient, 'send'>, name: string) {
    this.logger = logger;
    this.dynamodb = dynamodb;
    this.name = name;
  }

  public static instance(logger: Pick<Logger, 'error'>, dynamodbClient: DynamoDBClient, name: string): EventDB {
    const documentClient = DynamoDBDocumentClient.from(dynamodbClient, {
      marshallOptions: {
        convertEmptyValues: false,
        removeUndefinedValues: true,
        convertClassInstanceToMap: false,
      },
      unmarshallOptions: { wrapNumbers: false },
    });
    return new EventDB(logger, documentClient, name);
  }

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
  public static async createTable(dynamodbClient: DynamoDBClient, tableName: string): Promise<void> {
    await dynamodbClient.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [
          { AttributeName: 'subject', KeyType: KeyType.HASH },
          { AttributeName: 'time_type', KeyType: KeyType.RANGE },
        ],
        AttributeDefinitions: [
          { AttributeName: 'id', AttributeType: ScalarAttributeType.S },
          { AttributeName: 'type', AttributeType: ScalarAttributeType.S },
          { AttributeName: 'subject', AttributeType: ScalarAttributeType.S },
          { AttributeName: 'time', AttributeType: ScalarAttributeType.N },
          { AttributeName: 'time_type', AttributeType: ScalarAttributeType.S },
          { AttributeName: 'pk_all', AttributeType: ScalarAttributeType.S },
        ],
        GlobalSecondaryIndexes: [
          {
            // For LIST queries that fetch events in time order, regardless of subject.
            IndexName: 'AllEventsByTime',
            KeySchema: [
              { AttributeName: 'pk_all', KeyType: KeyType.HASH },
              { AttributeName: 'time', KeyType: KeyType.RANGE },
            ],
            Projection: { ProjectionType: ProjectionType.ALL },
          },
          {
            // For GET queries that fetch a specific event by its id.
            IndexName: 'AllEventsById',
            KeySchema: [{ AttributeName: 'id', KeyType: KeyType.HASH }],
            Projection: { ProjectionType: ProjectionType.KEYS_ONLY },
          },
        ],
        LocalSecondaryIndexes: [
          {
            // For queries like "get all events of this type for this subject"
            IndexName: 'SubjectEventsByType',
            KeySchema: [
              { AttributeName: 'subject', KeyType: KeyType.HASH },
              { AttributeName: 'type', KeyType: KeyType.RANGE },
            ],
            Projection: { ProjectionType: ProjectionType.ALL },
          },
        ],
      }),
    );
  }

  /** Creates a new event with the given attributes (source, type, subject, payload) and adds it to the database. */
  public async addNewEvents(candidates: EventCandidate[]): Promise<Event[]> {
    const errors: Error[] = [];
    const events: Event[] = [];

    for (const candidate of candidates) {
      const { event, error } = Cloudevent.new(candidate);
      if (error) {
        errors.push(error);
      } else {
        events.push(event);
      }
    }

    if (errors.length > 0) {
      throw new Error(`Failed to create events: ${errors.map((e) => e.message).join('; ')}`);
    }

    await this.addEvents(events);

    return events;
  }

  /** Writes multiple events in a single batch so they will be added to the database together. */
  public async addEvents(events: Event[]): Promise<void> {
    const errors: Error[] = [];
    const dynamoEvents: DynamoEvent[] = [];

    for (const event of events) {
      const { dynamoEvent, error } = Cloudevent.toDynamo(event);
      if (error) {
        errors.push(error);
      } else {
        dynamoEvents.push(dynamoEvent);
      }
    }

    if (errors.length > 0) {
      throw new Error(`Failed to convert events to DynamoDB format: ${errors.map((e) => e.message).join('; ')}`);
    }

    const putRequests = dynamoEvents.map((event) => ({
      PutRequest: { Item: event },
    }));

    await this.dynamodb.send(
      new BatchWriteCommand({
        RequestItems: { [this.name]: putRequests },
      }),
    );
  }

  /** Retrieves a single event by its ID. */
  public async fetchEvent(id: string): Promise<Event | null> {
    const indexResult = await this.dynamodb.send(
      new QueryCommand({
        TableName: this.name,
        IndexName: 'AllEventsById',
        KeyConditionExpression: 'id = :id',
        ExpressionAttributeValues: { ':id': id },
        Limit: 1,
      }),
    );

    const eventKeys = indexResult.Items?.[0];
    if (!eventKeys) {
      return null;
    }

    // since the index only projects the keys, we need to fetch the full item
    // using the primary key (subject + time_type)
    if (!eventKeys.subject || !eventKeys.time_type) {
      throw new Error('ID-index is missing subject or time_type attribute');
    }

    const result = await this.dynamodb.send(
      new QueryCommand({
        TableName: this.name,
        KeyConditionExpression: 'subject = :subject AND time_type = :time_type',
        ExpressionAttributeValues: { ':subject': eventKeys.subject, ':time_type': eventKeys.time_type },
        Limit: 1,
      }),
    );

    const dynamoEvent = result.Items?.[0];
    if (!dynamoEvent) {
      throw new Error('Event from ID-index could not be found in database.');
    }

    const { event, error } = Cloudevent.fromDynamo(dynamoEvent as DynamoEvent);
    if (error) {
      throw new Error(`Event is invalid: ${error.message}`);
    }

    return event;
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

  public async fetchEventsForSubjectAndType(subject: string, type: string): Promise<{ validItems: Event[]; invalidItems: Record<string, unknown>[] }> {
    const { validItems, invalidItems } = await this.paginatedQuery({
      IndexName: 'SubjectEventsByType',
      KeyConditionExpression: 'subject = :subject AND type = :type',
      ExpressionAttributeValues: { ':subject': subject, ':type': type },
      ScanIndexForward: true, // ascending order by time?
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
          const { event, error } = Cloudevent.fromDynamo(item as DynamoEvent);
          if (error) {
            this.logger.error('Failed to parse event from database', { error, item });
            invalidItems.push(item);
          } else {
            validItems.push(event);
          }
        }
      }

      lastEvaluatedKey = result.LastEvaluatedKey;
    } while (lastEvaluatedKey);

    return { validItems, invalidItems };
  }
}
