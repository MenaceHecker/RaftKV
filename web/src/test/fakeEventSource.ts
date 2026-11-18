import type { EventSourceCtor, EventSourceLike } from "../state/eventStream";

type Listener = (e: MessageEvent) => void;

/** A scripted EventSource: tests drive open/emit/error by hand. */
export class FakeEventSource implements EventSourceLike {
  static instances: FakeEventSource[] = [];
  static reset(): void {
    FakeEventSource.instances = [];
  }
  static get live(): FakeEventSource[] {
    return FakeEventSource.instances.filter((s) => s.readyState !== 2);
  }

  readyState = 0;
  onopen: ((e: Event) => void) | null = null;
  onerror: ((e: Event) => void) | null = null;
  readonly listeners = new Map<string, Listener[]>();
  constructor(readonly url: string) {
    FakeEventSource.instances.push(this);
  }
  addEventListener(name: string, fn: Listener): void {
    this.listeners.set(name, [...(this.listeners.get(name) ?? []), fn]);
  }
  close(): void {
    this.readyState = 2;
  }
  open(): void {
    this.readyState = 1;
    this.onopen?.(new Event("open"));
  }
  /** `data` is sent as-is when a string (to test malformed JSON), else JSON-encoded. */
  emit(name: string, data: unknown, id?: number): void {
    const payload = typeof data === "string" ? data : JSON.stringify(data);
    const ev = { data: payload, lastEventId: id === undefined ? "" : String(id) } as MessageEvent;
    for (const fn of this.listeners.get(name) ?? []) fn(ev);
  }
  /** What the browser does on a network blip: it keeps the source and retries itself. */
  errorReconnecting(): void {
    this.readyState = 0;
    this.onerror?.(new Event("error"));
  }
  /** What the browser does on an HTTP error or a CORS failure: the source is dead. */
  errorClosed(): void {
    this.readyState = 2;
    this.onerror?.(new Event("error"));
  }
}

export const FakeEventSourceCtor = FakeEventSource as unknown as EventSourceCtor;
