import { test } from 'vitest';
import { App, Stack } from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { EventDB } from './EventDB.js';
import { Key } from 'aws-cdk-lib/aws-kms';

test('synthesizes a non-prod stack', () => {
  const app = new App();
  const stack = new EventDB(app, 'TestEvents', {
    isProdEnv: false,
    subscriptionAccounts: [],
  });

  const template = Template.fromStack(stack);

  template.resourceCountIs('AWS::Lambda::Function', 1);
  template.resourceCountIs('AWS::DynamoDB::GlobalTable', 1);
  template.resourceCountIs('AWS::SNS::Topic', 1);
  template.resourceCountIs('AWS::Logs::LogGroup', 1);
  template.hasResource('AWS::DynamoDB::GlobalTable', {
    DeletionPolicy: 'Delete',
    UpdateReplacePolicy: 'Delete',
    Properties: { TableName: 'TestEvents' },
  });
  template.hasResource('AWS::SNS::Topic', {
    Properties: { TopicName: 'TestEventsTopic' },
  });
  template.hasResourceProperties('AWS::Logs::LogGroup', {
    RetentionInDays: 7, // ONE_WEEK
  });
});

test('synthesizes a prod PublishLambda with longer log retention and no dev flags', () => {
  const app = new App();
  const stack = new EventDB(app, 'TestEvents', {
    isProdEnv: true,
    subscriptionAccounts: [],
  });

  const template = Template.fromStack(stack);

  template.resourceCountIs('AWS::Lambda::Function', 1);
  template.resourceCountIs('AWS::DynamoDB::GlobalTable', 1);
  template.resourceCountIs('AWS::SNS::Topic', 1);
  template.resourceCountIs('AWS::Logs::LogGroup', 1);
  template.hasResource('AWS::DynamoDB::GlobalTable', {
    DeletionPolicy: 'Retain',
    UpdateReplacePolicy: 'Retain',
    Properties: { TableName: 'TestEvents' },
  });
  template.hasResource('AWS::SNS::Topic', {
    Properties: { TopicName: 'TestEventsTopic' },
  });
  template.hasResourceProperties('AWS::Logs::LogGroup', {
    RetentionInDays: 180, // SIX_MONTHS
  });
});

test('grants KMS encrypt/decrypt permissions when encryptionKey is provided', () => {
  const app = new App();
  const keyStack = new Stack(app, 'TestStack', {
    env: { account: '123456789012', region: 'eu-central-1' },
  });
  const key = new Key(keyStack, 'EncKey');
  const stack = new EventDB(app, 'TestEvents', {
    env: { account: '123456789012', region: 'eu-central-1' },
    isProdEnv: false,
    subscriptionAccounts: [],
    encryptionKey: key, // <-- enable encryption
  });

  const template = Template.fromStack(stack);

  template.hasResource('AWS::DynamoDB::GlobalTable', {
    Properties: {
      TableName: 'TestEvents',
      SSESpecification: { SSEEnabled: true, SSEType: 'KMS' },
      Replicas: Match.arrayWith([
        Match.objectLike({
          SSESpecification: { KMSMasterKeyId: { 'Fn::ImportValue': Match.anyValue() } },
        }),
      ]),
    },
  });
  template.hasResource('AWS::SNS::Topic', {
    Properties: {
      TopicName: 'TestEventsTopic',
      KmsMasterKeyId: { 'Fn::ImportValue': Match.anyValue() },
    },
  });
  template.hasResourceProperties('AWS::IAM::Policy', {
    PolicyDocument: Match.objectLike({
      Statement: Match.arrayWith([
        {
          Action: ['kms:Decrypt', 'kms:DescribeKey'],
          Effect: 'Allow',
          Resource: { 'Fn::ImportValue': Match.anyValue() },
        },
        {
          Action: ['kms:Decrypt', 'kms:GenerateDataKey*'],
          Effect: 'Allow',
          Resource: { 'Fn::ImportValue': Match.anyValue() },
        },
        {
          Action: ['kms:Decrypt', 'kms:Encrypt', 'kms:ReEncrypt*', 'kms:GenerateDataKey*'],
          Effect: 'Allow',
          Resource: { 'Fn::ImportValue': Match.anyValue() },
        },
      ]),
    }),
  });
});
