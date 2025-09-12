# Dynamo EventDB

An event-sourcing database wrapper for DynamoDB.
Adhering to the CNCF [CloudEvents Specification](https://cloudevents.io/).

This project has been heavily inspired by [@thenativeweb](https://github.com/thenativeweb)'s [EventsourcingDB](https://www.thenativeweb.io/products/eventsourcingdb).

## Features

- Stores events in AWS DynamoDB
- Simple API for adding and fetching events
- Validation and typing with Zod

## Usage

```ts
import { EventDB } from '@nicograef/dynamo-eventdb';
// ...initialize AWS DynamoDB Client...
const eventDB = EventDB.instance(dynamoClient, 'BookEvents');
await eventDB.addNewEvent({
  source: 'https://app.library.com',
  type: 'book.borrowed',
  subject: 'b123',
  payload: { userId: 'u456' },
});
```

## Installation

```bash
pnpm add @nicograef/dynamo-eventdb
# or
npm install @nicograef/dynamo-eventdb
```

Peer dependencies:

- `@aws-sdk/client-dynamodb`
- `@aws-sdk/lib-dynamodb`
- `zod`

## Development

Start DynamoDB locally:

```bash
docker run -p 8000:8000 amazon/dynamodb-local
```

## Testing

```bash
pnpm test
```

## License

MIT
