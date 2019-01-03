export interface IDB {
  connect(): Promise<void>;
  close(): Promise<void>;

  updated(data: any, refresh: boolean): Promise<string[]>;
  import_books_persons(updated: any): Promise<number>;
  import_byname(name: string, bulk_ops: any): Promise<number>;
}
