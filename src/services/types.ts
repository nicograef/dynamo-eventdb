import z from 'zod';

/**
 * Based on the [CNCF Cloudevents specification](https://cloudevents.io) version 1.0.2.
 * See https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
 */
export const EventSchema = z.object({
  id: z.string().min(3).describe('Identifies the event. Must be unique within the scope of the producer/source.'),
  type: z.string().min(3).describe('The type of event related to the source system and subject. E.g. com.library.book.borrowed:v1'),
  subject: z
    .string()
    .min(3)
    .describe(
      'The subject of the event in the context of the event producer (identified by source). E.g. the entity to which the event is primarily related. E.g. /users/12345',
    ),
  source: z.string().url().describe('The source of the event. Must be a valid URI-Reference. E.g. https://library.example.com'),
  time: z.number().int().min(1).describe('The timestamp of when the event occurred.'),
  data: z.record(z.string(), z.unknown()).describe('The event payload.'),
});
export type Event = z.infer<typeof EventSchema>;
