import { BillingMode, CreateTableCommand, KeyType, ScalarAttributeType, type DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, ScanCommand, QueryCommand, type ScanCommandInput, type QueryCommandInput } from '@aws-sdk/lib-dynamodb';
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
   * The table will have "subject" as partition key and "time" as sort key.
   * This will also serve as documentation on how the table should look like.
   *
   * We cannot use `time` as sort key because then two events created in the same millisecond
   * for the same subject would collide (i.e. override each other). Using `id` (a UUID) as sort key avoids this problem.
   */
  public static async createTable(dynamodbClient: DynamoDBClient, tableName: string): Promise<void> {
    await dynamodbClient.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [
          { AttributeName: 'subject', KeyType: KeyType.HASH },
          { AttributeName: 'id', KeyType: KeyType.RANGE },
        ],
        AttributeDefinitions: [
          { AttributeName: 'subject', AttributeType: ScalarAttributeType.S },
          { AttributeName: 'id', AttributeType: ScalarAttributeType.S },
        ],
        BillingMode: BillingMode.PAY_PER_REQUEST,
      }),
    );
  }

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
        Item: parsedEvent,
      }),
    );
  }

  /** Scans the whole DynamoDB table and returns all events. */
  public async fetchAllEvents(): Promise<{ validItems: Event[]; invalidItems: Record<string, unknown>[] }> {
    const { validItems, invalidItems } = await this.paginatedCommand((input: ScanCommandInput) => new ScanCommand(input));

    validItems.sort((a, b) => a.time - b.time);

    return { validItems, invalidItems };
  }

  /** Returns all events for the given subject in chronological order. */
  public async fetchEventsForSubject(subject: string): Promise<{ validItems: Event[]; invalidItems: Record<string, unknown>[] }> {
    const { validItems, invalidItems } = await this.paginatedCommand(
      (input: QueryCommandInput) =>
        new QueryCommand({
          ...input,
          KeyConditionExpression: 'subject = :subject',
          ExpressionAttributeValues: { ':subject': subject },
        }),
    );

    validItems.sort((a, b) => a.time - b.time);

    return { validItems, invalidItems };
  }

  private async paginatedCommand(
    createCommand: ((input: QueryCommandInput) => QueryCommand) | ((input: ScanCommandInput) => ScanCommand),
  ): Promise<{ validItems: Event[]; invalidItems: Record<string, unknown>[] }> {
    const validItems: Event[] = [];
    const invalidItems: Record<string, unknown>[] = [];

    let lastEvaluatedKey: Record<string, unknown> | undefined;

    do {
      const nextCommand = createCommand({ TableName: this.name, Limit: 100, ExclusiveStartKey: lastEvaluatedKey });
      const result = await this.dynamodb.send(nextCommand);

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
