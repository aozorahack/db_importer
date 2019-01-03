export interface IDB {
  connect(): Promise<void>;
  close(): Promise<void>;

  collection<TSchema>(name: string): {};

  updated(data: any, refresh: boolean): Promise<string[]>;
  import(updated: any): Promise<number>;
}
