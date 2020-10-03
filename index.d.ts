// MOST Web Framework 2.0 Codename Blueshift Copyright (c) 2017-2020 THEMOST LP

export declare class PoolDictionary {
    exists(key: any): boolean;
    push(key: any, value: any): void;
    pop(key: any): number;
    clear(): void;
    unshift(): any;
}

export declare class DataPool {
    constructor(options: any);
    static get(name: string): DataPool;
    createObject(): any;
    cleanup(callback: (error: Error) => void): void;
    cleanupAsync(): Promise<void>;
    newObject(callback: (error: Error, res?: any) => void): void;
    getObject(callback: (error: Error, res?: any) => void): void;
    getObjectAsync(): Promise<any>;
    releaseObject(obj: any, callback: (error: Error) => void): void;
    releaseObjectAsync(obj: any): Promise<void>;
}

export declare interface DataAdapterMigration {
    add: Array<any>;
    change?: Array<any>;
    appliesTo: string;
    version: string;
}

export declare class PoolAdapter {
    open(callback: (err: Error) => void): void;
    close(callback: (err: Error) => void): void;
    openAsync(): Promise<void>;
    closeAsync(): Promise<void>;
    createView(name: string, query: any, callback: (err: Error) => void): void;
    executeInTransaction(func: any, callback: (err: Error) => void): void;
    executeInTransactionAsync(func: Promise<any>): Promise<any>;
    migrate(obj: DataAdapterMigration, callback: (err: Error) => void): void;
    migrateAsync(obj: DataAdapterMigration): Promise<void>;
    selectIdentity(entity: string, attribute: string, callback: (err: Error, value: any) => void): void;
    execute(query: any, values: any, callback: (err: Error, value: any) => void): void;
    executeAsync(query: any, values: any): Promise<any>;
    lastIdentity(callback: (err: Error, value: any) => void): void;
}

export declare function createInstance(options: any): PoolAdapter;