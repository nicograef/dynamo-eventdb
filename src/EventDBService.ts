import { type EventDB } from './EventDB.js';
import { type Event } from './types.js';

export class EventDBService {
  private eventDB: EventDB;
  private source: string;

  constructor(eventDB: EventDB, source: string) {
    this.eventDB = eventDB;
    this.source = source;
  }

  public async addEvent(type: string, subject: string, payload: Record<string, unknown>): Promise<Event> {
    const event: Event = {
      id: crypto.randomUUID(),
      type,
      subject,
      source: this.source,
      time: Date.now(),
      data: payload,
    };

    await this.eventDB.addEvent(event);

    return event;
  }
}
