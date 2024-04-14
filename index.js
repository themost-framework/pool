// MOST Web Framework 2.0 Codename Blueshift Copyright (c) 2017-2020 THEMOST LP

const { createPool } = require('generic-pool');
const { Args, TraceUtils } = require('@themost/common');
const { AsyncEventEmitter } = require('@themost/events');

const getConfigurationMethod = Symbol('getConfiguration');
const pools = Symbol('pools');


/**
 * @class
 * @property {*} options
 */
class GenericPoolFactory {
    /**
     * @constructor
     * @param {GenericPoolOptions=} options
     */
    constructor(options) {
        this.options = Object.assign({}, options);
    }

    create() {
        //if local adapter module has been already loaded
        if (typeof this._adapter !== 'undefined') {
            //create adapter instance and return
            return this._adapter.createInstance(this.options.adapter.options);
        }

        this.options = this.options || {};
        if (typeof this[getConfigurationMethod] !== 'function') {
            throw new TypeError('Configuration getter must be a function.');
        }
        /**
         * @type {ConfigurationBase}
         */
        let configuration = this[getConfigurationMethod]();
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
        return this._adapter.createInstance(this.options.adapter.options);
    }

    destroy(adapter) {
        if (adapter) {
            return adapter.close();
        }
    }

}

/**
 * @class
 */
class GenericPoolAdapter {

    static [pools] = {};
    static acquired = new AsyncEventEmitter();
    static released = new AsyncEventEmitter();

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
                return GenericPoolAdapter[pools][self.options.pool];
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
        this.pool._factory[getConfigurationMethod] = getConfigurationFunc;
    }

    /**
     * @private
     * @param {GenericPoolAdapterCallback} callback
     */
    open(callback) {
        const self = this;
        if (self.base) {
            return self.base.open(callback);
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
            //add lastIdentity() method by assigning base.lastIdentity
            if (self.base && typeof self.base.lastIdentity === 'function') {
                Object.assign(self, {
                    lastIdentity(callback) {
                        return this.base.lastIdentity(callback);
                    }
                });
            }
            //add nextIdentity() method by assigning base.nextIdentity
            if (self.base && typeof self.base.nextIdentity === 'function') {
                Object.assign(self, {
                    nextIdentity(entity, attribute, callback) {
                        return this.base.nextIdentity(entity, attribute, callback);
                    }
                });
            }
            // assing extra property for current pool
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
        return GenericPoolAdapter[pools][name];
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
            // remove lastIdentity() method
            if (typeof this.lastIdentity === 'function') {
                delete this.lastIdentity;
            }
            // remove nextIdentity() method
            if (typeof this.nextIdentity === 'function') {
                delete this.nextIdentity;
            }
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
     * @param {function} callback 
     * @returns {void}
     */
    tryClose(callback) {
        // if an active transaction exists, do not close the connection
        if (this.transaction) {
            return callback();
        }
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
        return self.open(function (err) {
            if (err) {
                return callback(err);
            }
            return self.base.execute(query, values, function(err, results) {
                // try close connection
                self.tryClose(function() {
                    return callback(err, results);
                });
            });
        });
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
     * @param batch {*}
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
     * @param {QueryExpression} query The query expression that represents the database vew
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
        if (self.transaction) {
            return executeFunc.call(self, function(err) {
                callback(err);
            });
        }
        return self.open(function (err) {
            if (err) {
                return callback(err);
            }
            self.transaction = true;
            return self.base.executeInTransaction(executeFunc, function(err) {
                self.transaction = false;
                return self.tryClose(function() {
                    return callback(err);
                });
            });
        });
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
     * @type {*}
     */
    // upgrade from 2.2.x
    if (typeof options.size === 'number') {
        options.max = options.size
    }
    if (typeof options.timeout === 'number') {
        options.acquireTimeoutMillis = options.timeout
    }
    let pool = GenericPoolAdapter[pools][name];
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
        GenericPoolAdapter[pools][name] = pool;
        TraceUtils.debug(`GenericPoolAdapter: createPool() => name: ${name}, min: ${pool.min}, max: ${pool.max}`);
    }
    return new GenericPoolAdapter({ pool: name });
}

process.on('exit', function () {
    if (GenericPoolAdapter[pools] == null) {
        return;
    }
    try {
        const keys = Object.keys(GenericPoolAdapter[pools]);
        keys.forEach(key => {
            if (Object.hasOwnProperty.call(GenericPoolAdapter[pools], key) === false) {
                return;
            }
            try {
                TraceUtils.log(`GenericPoolAdapter: Cleaning up data pool ${key}`);
                const pool = GenericPoolAdapter[pools][key];
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
