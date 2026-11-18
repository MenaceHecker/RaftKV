export interface RecordOptions {
  gateway: string;
  seconds: number;
  fetch: typeof fetch;
  sleep: (ms: number) => Promise<void>;
  now: () => number;
}
export function recordSession(opts: RecordOptions): Promise<string[]>;
export function writeRecording(path: string, lines: string[]): Promise<void>;
