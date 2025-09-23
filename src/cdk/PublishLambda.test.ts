import { test, vi } from 'vitest';
import { Stack } from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { PublishLambda } from './PublishLambda.js';
import { Key } from 'aws-cdk-lib/aws-kms';
import { AttributeType, StreamViewType, Table } from 'aws-cdk-lib/aws-dynamodb';

test('synthesizes a non-prod PublishLambda with expected defaults', () => {
  const stack = new Stack();
  const table = new Table(stack, 'Table', {
    partitionKey: { name: 'id', type: AttributeType.STRING },
    stream: StreamViewType.NEW_IMAGE,
  });
  new PublishLambda(stack, {
    isProdEnv: false,
    eventTopic: { topicArn: 'arn:aws:eventTopic', grantPublish: vi.fn() },
    eventTable: table,
  });

  const template = Template.fromStack(stack);

  template.resourceCountIs('AWS::Lambda::Function', 1);
  template.hasResourceProperties('AWS::Lambda::Function', {
    Description: 'Lambda to publish events to the event eventTopic',
    Runtime: 'nodejs22.x',
    MemorySize: 512,
    Timeout: 5,
    Environment: {
      Variables: Match.objectLike({
        LOG_LEVEL: 'INFO',
        NODE_OPTIONS: '--enable-source-maps',
        POWERTOOLS_DEV: 'true',
        POWERTOOLS_LOGGER_LOG_EVENT: 'true',
        ENV_TOPIC_ARN: 'arn:aws:eventTopic',
      }),
    },
  });
  template.resourceCountIs('AWS::Lambda::EventSourceMapping', 1);
  template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
    StartingPosition: 'LATEST',
  });
  template.resourceCountIs('AWS::Logs::LogGroup', 1);
  template.hasResourceProperties('AWS::Logs::LogGroup', {
    RetentionInDays: 7, // ONE_WEEK
  });
});

test('synthesizes a prod PublishLambda with longer log retention and no dev flags', () => {
  const stack = new Stack();
  const table = new Table(stack, 'Table', {
    partitionKey: { name: 'id', type: AttributeType.STRING },
    stream: StreamViewType.NEW_IMAGE,
  });
  new PublishLambda(stack, {
    isProdEnv: true,
    eventTopic: { topicArn: 'arn:aws:eventTopic', grantPublish: vi.fn() },
    eventTable: table,
  });

  const template = Template.fromStack(stack);

  template.resourceCountIs('AWS::Lambda::Function', 1);
  template.hasResourceProperties('AWS::Lambda::Function', {
    Description: 'Lambda to publish events to the event eventTopic',
    Runtime: 'nodejs22.x',
    MemorySize: 512,
    Timeout: 5,
    Environment: {
      Variables: Match.objectLike({
        LOG_LEVEL: 'INFO',
        NODE_OPTIONS: '--enable-source-maps',
        POWERTOOLS_DEV: 'false',
        POWERTOOLS_LOGGER_LOG_EVENT: 'false',
        ENV_TOPIC_ARN: 'arn:aws:eventTopic',
      }),
    },
  });
  template.resourceCountIs('AWS::Lambda::EventSourceMapping', 1);
  template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
    StartingPosition: 'LATEST',
  });
  template.resourceCountIs('AWS::Logs::LogGroup', 1);
  template.hasResourceProperties('AWS::Logs::LogGroup', {
    RetentionInDays: 180, // SIX_MONTHS
  });
});

test.skip('grants KMS encrypt/decrypt permissions when encryptionKey is provided', () => {
  const stack = new Stack();
  const key = new Key(stack, 'EncKey');
  const table = new Table(stack, 'Table', {
    partitionKey: { name: 'id', type: AttributeType.STRING },
    stream: StreamViewType.NEW_IMAGE,
  });
  new PublishLambda(stack, {
    isProdEnv: false,
    eventTopic: { topicArn: 'arn:aws:eventTopic', grantPublish: vi.fn() },
    eventTable: table,
    encryptionKey: key,
  });

  const template = Template.fromStack(stack);

  // Look for an IAM Policy statement that includes KMS actions referencing the key
  template.hasResourceProperties('AWS::IAM::Policy', {
    PolicyDocument: Match.objectLike({
      Statement: Match.arrayWith([
        Match.objectLike({
          Action: Match.arrayWith(['kms:Encrypt', 'kms:Decrypt', 'kms:GenerateDataKey*', 'kms:DescribeKey']),
          Effect: 'Allow',
          Resource: { 'Fn::GetAtt': Match.arrayWith(['EncKey', 'Arn']) },
        }),
      ]),
    }),
  });
});
