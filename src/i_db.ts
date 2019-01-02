export interface DBCollection<TSchema> {
}

export interface IDB {
  connect(): Promise<void>;
  close(): Promise<void>;

  collection<TSchema>(name: string): DBCollection<TSchema>;

  updated(data: any, refresh: boolean): Promise<Array<string>>;
  import(updated: any): Promise<number>;
}
