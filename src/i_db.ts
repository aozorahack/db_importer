export interface IDB {
    connect(): Promise<void>;
    close(): Promise<void>;

    updated(data: any, refresh: boolean): Promise<string[]>;
    import_books_persons(updated: any): Promise<number>;
    import_byname(name: string, bulk_ops: any): Promise<number>;

    find_bookids(collection: string, query: object): Promise<number[]>;
    find_one<T>(collection: string, query: object): Promise<T>;

    replace_one(collection: string, filter: object, doc: object, options: object): Promise<object>;

    create_index(collection: string, spec: object, options: object): Promise<string>;
}
