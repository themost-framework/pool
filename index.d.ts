// MOST Web Framework 2.0 Codename Blueshift Copyright (c) 2017-2020 THEMOST LP

import {ConfigurationBase, DataAdapterDatabase, DataAdapterTable, DataAdapterView} from '@themost/common';
import {AsyncEventEmitter} from '@themost/events';

declare type GenericPoolAdapterCallback = (err?: Error) => void;

export declare interface GenericPoolOptions {
    adapter: string;
    disableParallelTransactions?: boolean;
    max?: number;
    min?: number;
    maxWaitingClients?: number;
    testOnBorrow?: boolean;
    acquireTimeoutMillis?: number;
    fifo?: boolean;
    priorityRange?: number;
    autostart?: boolean;
    evictionRunIntervalMillis?: number;
    softIdleTimeoutMillis?: number;
    idleTimeoutMillis?: number;
}

export declare interface GenericPoolAdapterOptions {
    name: string;
    invariantName: string;
    default?: boolean;
    options: GenericPoolOptions;
}

declare interface ApplicationDataConfiguration {
    adapters: Array<any>;
    getAdapterType(name: string): any;
}

export declare interface GenericPool {
    acquire(): Promise<any>;
    release(resource: any): Promise<void>;
}

export declare class GenericPoolAdapter {

    static acquired: AsyncEventEmitter<{ name: string, target: GenericPool, borrowed: number, pending: number }>;
    static released: AsyncEventEmitter<{ name: string, target: GenericPool, borrowed: number, pending: number }>;

    static pool(name: string): GenericPool;
    constructor(options: GenericPoolAdapterOptions);
    open(callback: GenericPoolAdapterCallback): void;
    openAsync(): Promise<void>;
    close(callback: GenericPoolAdapterCallback): void;
    closeAsync(): Promise<void>;
    createView(name: string, query: any, callback: GenericPoolAdapterCallback): void;
    executeInTransaction(func: any, callback: GenericPoolAdapterCallback): void;
    executeInTransactionAsync(func: Promise<any>): Promise<any>;
    migrate(obj: any, callback: GenericPoolAdapterCallback): void;
    migrateAsync(obj: any): Promise<void>;
    selectIdentity(entity: string, attribute: string, callback: (err: Error, value: any) => void): void;
    execute(query: any, values: any, callback: (err: Error, value: any) => void): void;
    executeAsync(query: any, values: any): Promise<any>;
    table(name: string): DataAdapterTable;
    view(name: string): DataAdapterView;
    database(name: string): DataAdapterDatabase;

}

export declare function createInstance(options: any): GenericPoolAdapter;
