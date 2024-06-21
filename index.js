// MOST Web Framework 2.0 Codename Blueshift Copyright (c) 2017-2020 THEMOST LP

const { createPool } = require('generic-pool');
const { Args, TraceUtils } = require('@themost/common');
const { AsyncEventEmitter } = require('@themost/events');

class GenericPoolFactory {
    /**
     * @constructor
     * @param {GenericPoolOptions=} options
     */
    constructor(options) {
        this.options = Object.assign({}, options);
        this._adapter = null;
        this.getConfiguration = null;
    }

    createSync() {
        const { options } = this.options.adapter;
        // if local adapter module has been already loaded
        if (this._adapter) {
            // create adapter instance and return
            return this._adapter.createInstance(options)
        }

        this.options = this.options || {};
        if (typeof this.getConfiguration !== 'function') {
            throw new TypeError('Configuration getter must be a function.');
        }
        /**
         * @type {import('@themost/common').ConfigurationBase}
         */
        let configuration = this.getConfiguration();
        if (configuration == null) {
            throw new TypeError('Configuration cannot be empty at this context.');
        }
        /**
         * @type {ApplicationDataConfiguration}
         */
        let dataConfiguration = configuration.getStrategy(function DataConfigurationStrategy() {
            //
        });
        if (dataConfiguration == null) {
            throw new TypeError('Data configuration cannot be empty at this context.');
        }
        if (typeof dataConfiguration.getAdapterType !== 'function') {
            throw new TypeError('Data configuration adapter getter must be a function.');
        }
        let adapter = dataConfiguration.adapters.find((x) => {
            return x.name === this.options.adapter;
        });
        if (adapter == null) {
            throw new TypeError('Child data adapter cannot be found.');
        }
        this._adapter = dataConfiguration.getAdapterType(adapter.invariantName);
        //set child adapter
        this.options.adapter = adapter;
        //get child adapter
        return this._adapter.createInstance(adapter.options);
    }

    create() {
        return new Promise((resolve, reject) => {
            try {
                const connection1 = this.createSync();
                return resolve(connection1);
            } catch(err) {
                return reject(err);
            }
        });
        
    }

    /**
     * @param adapter
     * @returns {Promise<void>}
     */
    destroy(adapter) {
        return new Promise((resolve, reject) => {
            try {
                if (adapter) {
                    return adapter.close(() => {
                        return resolve();
                    });
                }
                return resolve();
            } catch (err) {
                return reject(err);
            }
        });
    }

}

/**
 * @class
 */
class GenericPoolAdapter {

    static pools = {};
    static acquired = new AsyncEventEmitter();
    static released = new AsyncEventEmitter();
    executing = 0;
    transaction = false;

    /**
     * @constructor
     * @property {*} base
     * @property {GenericPoolFactory} pool
     */
    constructor(options) {
        this.options = options;
        const self = this;
        Object.defineProperty(this, 'pool', {
            get: function () {
                return GenericPoolAdapter.pools[self.options.pool];
            },
            configurable: false,
            enumerable: false
        });
    }

    // noinspection JSUnusedGlobalSymbols
    /**
     * Assigns the given application configuration getter to current object.
     * @param {Function} getConfigurationFunc
     */
    hasConfiguration(getConfigurationFunc) {
        /**
         * @type {{_factory: *}}
         */
        const { pool } = this;
        pool._factory.getConfiguration = getConfigurationFunc;
        // try to assign prototype methods
        if (typeof getConfigurationFunc === 'function') {
            /**
             * @type {GenericPoolFactory}
             */
            const factory = pool._factory;
            const adapter = factory.createSync();
            // implement methods
            // noinspection JSUnresolvedReference
            const {
                lastIdentity, lastIdentityAsync, nextIdentity
            } = adapter;
            [
                lastIdentity, lastIdentityAsync, nextIdentity
            ].filter((func) => typeof func === 'function').filter((func) => {
                return typeof this[func.name] === 'undefined';
            }).forEach((func) => {
                Object.assign(this, {
                    [func.name]: function() {
                        return func.apply(this.base, Array.from(arguments));
                    }
                });
            });
        }
    }

    /**
     * @private
     * @param {GenericPoolAdapterCallback} callback
     */
    open(callback) {
        const self = this;
        if (self.base) {
            return self.base.open((callback));
        }
        // get object from pool
        self.pool.acquire().then(result => {
            const { borrowed, pending } = self.pool;
            GenericPoolAdapter.acquired.emit({
                target: self.pool,
                name: self.options.pool,
                borrowed,
                pending
            });
            // set base adapter
            self.base = result;
            // assign extra property for current pool
            Object.defineProperty(self.base, 'pool', {
                configurable: true,
                enumerable: true,
                writable: false,
                value: self.options && self.options.pool
            });
            return self.base.open(callback);
        }).catch(err => {
            return callback(err);
        });
    }

    /**
     * @type {string} name
     * @returns {GenericPool}
     */
    static pool(name) {
        return GenericPoolAdapter.pools[name];
    }

    /**
     * Opens a database connection
     */
    openAsync() {
        return new Promise((resolve, reject) => {
            return this.open(err => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    /**
     * Closes the underlying database connection
     * @param {GenericPoolAdapterCallback=} callback
     */
    close(callback) {
        callback = callback || function () { };
        if (this.base) {
            // return object to pool
            this.pool.release(this.base);
            const { borrowed, pending } = this.pool;
            GenericPoolAdapter.released.emit({
                target: this.pool,
                name: this.options.pool,
                borrowed,
                pending
            });
            // destroy local object
            delete this.base;
        }
        // exit
        return callback();
    }

    /**
     * Closes the current database connection
     */
    closeAsync() {
        return new Promise((resolve, reject) => {
            return this.close(err => {
                if (err) {
                    return reject(err);
                }
                return resolve();
            });
        });
    }

    /**
     * @param {function(Error=)} callback
     */
    tryClose(callback) {
        // if executing procedures are more than 0
        if (this.executing > 0) {
            // decrease executing procedures
            this.executing--;
        }
        // if transaction is active
        if (this.transaction) {
            // do nothing
            // (do not try to close connection because transaction is active and will be closed later)
            return callback();
        }
        // if executing procedures are more than 0
        if (this.executing > 0) {
            // do nothing (there are still executing procedures)
            return callback();
        }
        // otherwise, auto-close connection
        return this.close(callback);
    }

    /**
     * Executes a query and returns the result as an array of objects.
     * @param {string|*} query
     * @param {*} values
     * @param {Function} callback
     */
    execute(query, values, callback) {
        const self = this;
        try {
            // increase executing procedures when a transaction is not active
            // this operation is required to prevent connection auto-close during async operations
            if (!self.transaction) {
                self.executing++;
            }
            return self.open((err) => {
                if (err) {
                    // try to close connection to release connection immediately
                    self.tryClose(() => {
                        return callback(err);
                    });
                }
                return self.base.execute(query, values, (err, results) => {
                    // try close connection
                    self.tryClose(() => {
                        return callback(err, results);
                    });
                });
            });
        } catch (error) {
            self.tryClose(() => {
                return callback(error);
            });
        }
    }

    /**
     * @param query {*}
     * @param values {*}
     * @returns Promise<any>
     */
    executeAsync(query, values) {
        return new Promise((resolve, reject) => {
            return this.execute(query, values, (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve(res);
            });
        });
    }

    /**
     * Executes an operation against database and returns the results.
     * @param _batch {*}
     * @param callback {Function=}
     */
    executeBatch(_batch, callback) {
        callback(new Error('This method is obsolete. Use DataAdapter.executeInTransaction() instead.'));
    }

    /**
     * Produces a new identity value for the given entity and attribute.
     * @param entity {String} The target entity name
     * @param attribute {String} The target attribute
     * @param callback {Function=}
     */
    selectIdentity(entity, attribute, callback) {
        const self = this;
        self.open(function (err) {
            if (err) { return callback(err); }
            if (typeof self.base.selectIdentity !== 'function') {
                return callback(new Error('This method is not yet implemented. The base DataAdapter object does not implement this method..'));
            }
            self.base.selectIdentity(entity, attribute, callback);
        });
    }

    /**
     * Creates a database view if the current data adapter supports views
     * @param {string} name A string that represents the name of the view to be created
     * @param {import('@themost/query').QueryExpression} query The query expression that represents the database vew
     * @param {Function} callback A callback function to be called when operation will be completed.
     */
    createView(name, query, callback) {
        const self = this;
        self.open(function (err) {
            if (err) { return callback(err); }
            self.base.createView(name, query, callback);
        });
    }

    /**
     * Begins a transactional operation by executing the given function
     * @param executeFunc {Function} The function to execute
     * @param callback {Function} The callback that contains the error -if any- and the results of the given operation
     */
    executeInTransaction(executeFunc, callback) {
        const self = this;
        try {
            if (self.transaction) {
                return executeFunc((err) => {
                    return callback(err);
                });
            }
            return self.open((err) => {
                if (err) {
                    return callback(err);
                }
                // validate transaction during async operation
                if (self.transaction) {
                    if (self.options.disableParallelTransactions === true) {
                        return callback(new Error('Parallel transactions detected but this operation is not not allowed based on current configuration.'));
                    }
                    return executeFunc((err) => {
                        return callback(err);
                    });
                }
                self.transaction = true;
                return self.base.executeInTransaction(executeFunc, (err) => {
                    self.transaction = false;
                    return self.tryClose(() => {
                        return callback(err);
                    });
                });
            });
        } catch (error) {
            return callback(error);
        }
    }

    /**
     * Begins a data transaction and executes the given function
     * @param func {Function}
     */
    executeInTransactionAsync(func) {
        return new Promise((resolve, reject) => {
            return this.executeInTransaction((callback) => {
                return func.call(this).then(res => {
                    return callback(null, res);
                }).catch(err => {
                    return callback(err);
                });
            }, (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve(res);
            });
        });
    }

    /**
     *
     * @param {*} obj  An Object that represents the data model scheme we want to migrate
     * @param {GenericPoolAdapterCallback} callback
     */
    migrate(obj, callback) {
        const self = this;
        return self.open(function (err) {
            if (err) {
                return callback(err);
            }
            return self.base.migrate(obj, callback);
        });
    }

    /**
     * 
     * @param {*} obj 
     * @returns {*}
     */
    migrateAsync(obj) {
        return new Promise((resolve, reject) => {
            this.migrate(obj, (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve(res);
            });
        });
    }

    /**
     * @param name
     */
    table(name) {
        const self = this;
        return {
            /**
             * @param {function(Error,Boolean=)} callback
             */
            exists: function (callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.table(name).exists(callback);
                });
            },
            existsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.exists((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {function(Error,string=)} callback
             */
            version: function (callback) {
               self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.table(name).version(callback);
                });
            },
            versionAsync: function () {
                return new Promise((resolve, reject) => {
                    this.version((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            /**
             * @param {function(Error,{name:string, ordinal:number, type:*, size:number, nullable:boolean }[]=)} callback
             */
            columns: function (callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.table(name).columns(callback);
                });
            },
            columnsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.columns((err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * @param {Array<*>} fields
             * @param callback
             */
            create: function (fields, callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.table(name).create(fields, callback);
                });
            },
            createAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.create(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * Alters the table by adding an array of fields
             * @param {Array<*>} fields
             * @param callback
             */
            add: function (fields, callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.table(name).add(fields, callback);
                });
            },
            addAsync: function (fields) {
                return new Promise((resolve, reject) => {
                    this.add(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * Alters the table by modifying an array of fields
             * @param {Array<*>} fields
             * @param callback
             */
            change: function (fields, callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.table(name).change(fields, callback);
                });
            },
            changeAsync(fields) {
                return new Promise((resolve, reject) => {
                    this.change(fields, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            }
        }
    }

    view(name) {
        const self = this;
        return {
            /**
             * @param {function(Error,Boolean=)} callback
             */
            exists: function (callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.view(name).exists(callback);
                });
            },
            existsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.exists((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            create: function (query, callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.view(name).create(query, callback);
                });
            },
            createAsync: function (query) {
                return new Promise((resolve, reject) => {
                    this.create(query, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * @param {Function} callback
             */
            drop: function (callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.view(name).drop(callback);
                });
            },
            dropAsync: function () {
                return new Promise((resolve, reject) => {
                    this.drop((err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            }
        }
    }

    database(name) {
        const self = this;
        return {
            exists: function (callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.database(name).exists(callback);
                });
            },
            existsAsync: function () {
                return new Promise((resolve, reject) => {
                    this.exists((err, value) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(value);
                    });
                });
            },
            create: function (callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.database(name).create(callback);
                });
            },
            createAsync: function () {
                return new Promise((resolve, reject) => {
                    this.create((err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            }
        }
    }

    indexes(name) {
        const self = this;
        return {
            list: function (callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.indexes(name).list(callback);
                });
            },
            listAsync: function () {
                return new Promise((resolve, reject) => {
                    this.list((err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * @param {string} indexName
             * @param {Array|string} columns
             * @param {Function} callback
             */
            create: function (indexName, columns, callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.indexes(name).create(indexName, columns, callback);
                });
            },
            /**
             * @param {string} indexName
             * @param {Array|string} columns
             */
            createAsync(indexName, columns) {
                return new Promise((resolve, reject) => {
                    this.create(indexName, columns, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            },
            /**
             * @param {string} indexName
             * @param {Function} callback
             */
            drop(indexName, callback) {
                self.open(function (err) {
                    if (err) {
                        return callback(err);
                    }
                    return self.base.indexes(name).drop(indexName, callback);
                });
            },
            dropAsync: function (indexName) {
                return new Promise((resolve, reject) => {
                    this.drop(indexName, (err, res) => {
                        if (err) {
                            return reject(err);
                        }
                        return resolve(res);
                    });
                });
            }
        }
    }

}
/**
 * @param {GenericPoolOptions} options
 */
function createInstance(options) {
    Args.check(options.adapter != null, 'Invalid argument. The target data adapter is missing.');
    //get adapter's name
    let name;
    if (typeof options.adapter === 'string') {
        name = options.adapter;
    }
    Args.check(name != null, 'Invalid argument. The target data adapter name is missing.');
    /**
     * @type {{size:number=,timeout:number=}|*}
     */
    const testOptions = options;
    // upgrade from 2.2.x
    if (typeof testOptions.size === 'number') {
        options.max = testOptions.size;
    }
    if (typeof testOptions.timeout === 'number') {
        options.acquireTimeoutMillis = testOptions.timeout;
    }
    let pool = GenericPoolAdapter.pools[name];
    if (pool == null) {
        if (typeof options.min === 'number') {
            TraceUtils.warn('GenericPoolAdapter: The min property is not supported and will be ignored.');
            delete options.min;
        }
        //create new pool with the name specified in options
        pool = createPool(new GenericPoolFactory(options), Object.assign({
            // set default max size to 25
            max: 25
        }, options));
        GenericPoolAdapter.pools[name] = pool;
        TraceUtils.debug(`GenericPoolAdapter: createPool() => name: ${name}, min: ${pool.min}, max: ${pool.max}`);
    }
    return new GenericPoolAdapter({
        pool: name,
        disableParallelTransactions: !!options.disableParallelTransactions,
    });
}

process.on('exit', function () {
    if (GenericPoolAdapter.pools == null) {
        return;
    }
    try {
        const keys = Object.keys(GenericPoolAdapter.pools);
        keys.forEach(key => {
            if (Object.hasOwnProperty.call(GenericPoolAdapter.pools, key) === false) {
                return;
            }
            try {
                TraceUtils.info(`GenericPoolAdapter: Cleaning up data pool ${key}`);
                const pool = GenericPoolAdapter.pools[key];
                if (pool == null) {
                    return;
                }
                pool.drain().then(function () {
                    pool.clear();
                });
            }
            catch (err) {
                TraceUtils.error(`GenericPoolAdapter: An error occurred while cleaning up ${key} pool`);
                TraceUtils.error(err);
            }
        });
    }
    catch (err) {
        TraceUtils.error('GenericPoolAdapter: An error occurred while cleaning up pools');
        TraceUtils.error(err);
    }

});

module.exports = {
    GenericPoolAdapter,
    createInstance
};
