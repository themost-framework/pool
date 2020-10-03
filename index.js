// MOST Web Framework 2.0 Codename Blueshift Copyright (c) 2017-2020 THEMOST LP
const _ = require('lodash');
const async = require('async');
const { TraceUtils } = require('@themost/common');

const HASH_CODE_LENGTH = 24;
const getConfigurationMethod = Symbol('getConfiguration');

const pools = Symbol('pools');
/**
 * @function
 * @param min
 * @param max
 * @returns {*}
 */
function randomInt(min, max) {
    return Math.floor(Math.random() * max) + min;
}

function randomString(length) {

    length = length || 16;
    const chars = "abcdefghkmnopqursuvwxz2456789ABCDEFHJKLMNPQURSTUVWXYZ";
    let str = "";
    for (let i = 0; i < length; i++) {
        str += chars.substr(randomInt(0, chars.length - 1), 1);
    }
    return str;
}

function randomHex(length) {

    const buffer = Buffer.from(randomString(length));
    return buffer.toString('hex');
}

function log(data) {
    TraceUtils.log(data);
    if (data.stack) {
        TraceUtils.log(data.stack);
    }
}

function debug(data) {
    if (process.env.NODE_ENV === 'development')
        log(data);
}

/**
 * @class PoolDictionary
 * @constructor
 */
class PoolDictionary {
    constructor() {
        let _length = 0;
        Object.defineProperty(this, 'length', {
            get: function () {
                return _length;
            },
            set: function (value) {
                _length = value;
            }, configurable: false, enumerable: false
        });
    }

    exists(key) {
        return Object.prototype.hasOwnProperty.call(this, key);
    }

    push(key, value) {
        if (Object.prototype.hasOwnProperty.call(this, key)) {
            this[key] = value;
        }
        else {
            this[key] = value;
            this.length += 1;
        }
    }

    pop(key) {
        if (Object.prototype.hasOwnProperty.call(this, key)) {
            delete this[key];
            this.length -= 1;
            return 1;
        }
        return 0;
    }

    clear() {
        const self = this, keys = _.keys(this);
        _.forEach(keys, function (x) {
            if (Object.prototype.hasOwnProperty.call(self, x)) {
                delete self[x];
                self.length -= 1;
            }
        });
        this.length = 0;
    }

    unshift() {
        for (const key in this) {
            if (Object.prototype.hasOwnProperty.call(this, key)) {
                const value = this[key];
                delete this[key];
                this.length -= 1;
                return value;
            }
        }
    }
}

/**
 * @class
 * @property {*} options
 */
class DataPool {
    /**
     * @constructor
     * @param {*=} options
     */
    constructor(options) {
        this.options = Object.assign({ size: 20, reserved: 2, timeout: 30000, lifetime: 1200000 }, options);
        /**
         * A collection of objects which represents the available pooled data adapters.
         */
        this.available = new PoolDictionary();
        /**
         * A collection of objects which represents the pooled data adapters that are currently in use.
         */
        this.inUse = new PoolDictionary();
        /**
         * An array of listeners that are currently waiting for a pooled data adapter.
         * @type {Function[]}
         */
        this.listeners = [];
        //set default state to active
        this.state = 'active';

    }

    createObject() {
        //if local adapter module has been already loaded
        if (typeof this.adapter_ !== 'undefined') {
            //create adapter instance and return
            return this.adapter_.createInstance(this.options.adapter.options);
        }

        this.options = this.options || {};
        if (typeof this[getConfigurationMethod] !== 'function') {
            throw new TypeError('Configuration getter must be a function.');
        }
        /**
         * @type {ConfigurationBase}
         */
        let configuration = this[getConfigurationMethod]();
        if (_.isNil(configuration)) {
            throw new TypeError('Configuration cannot be empty at this context.');
        }
        let dataConfiguration = configuration.getStrategy(function DataConfigurationStrategy() {
            //
        });
        if (_.isNil(dataConfiguration)) {
            throw new TypeError('Data configuration cannot be empty at this context.');
        }
        if (typeof dataConfiguration.getAdapterType !== 'function') {
            throw new TypeError('Data configuration adapter getter must be a function.');
        }
        let adapter = dataConfiguration.adapters.find((x) => {
            return x.name === this.options.adapter;
        });
        if (_.isNil(adapter)) {
            throw new TypeError('Child data adapter cannot be found.');
        }
        this.adapter_ = dataConfiguration.getAdapterType(adapter.invariantName);
        //set child adapter
        this.options.adapter = adapter;
        //get child adapter
        return this.adapter_.createInstance(this.options.adapter.options);
    }

    cleanup(callback) {
        try {
            const self = this;
            self.state = 'paused';
            const keys = _.keys(self.available);
            async.eachSeries(keys, function (key, cb) {
                const item = self.available[key];
                if (typeof item === 'undefined' || item === null) { return cb(); }
                if (typeof item.close === 'function') {
                    item.close(function () {
                        cb();
                    });
                }
            }, function (err) {
                callback(err);
                //clear available collection
                _.forEach(keys, function (key) {
                    delete self.available[key];
                });
                self.state = 'active';
            });
        }
        catch (e) {
            callback(e);
        }
    }

    /**
     * Queries data adapter lifetime in order to release an object which exceeded the defined lifetime limit.
     * If such an object exists, releases data adapter and creates a new one.
     * @private
     * @param {Function} callback
     */
    queryLifetimeForObject(callback) {
        const self = this;
        const keys = _.keys(self.inUse);
        let newObj;
        if (keys.length === 0) { return callback(); }
        if (self.options.lifetime > 0) {
            const nowTime = (new Date()).getTime();
            async.eachSeries(keys, function (hashCode, cb) {
                const obj = self.inUse[hashCode];
                if (typeof obj === 'undefined' || obj === null) {
                    return cb();
                }
                if (nowTime > (obj.createdAt.getTime() + self.options.lifetime)) {
                    if (typeof obj.close !== 'function') {
                        return cb();
                    }
                    //close data adapter (the client which is using this adapter may get an error for this, but this data adapter has been truly timed out)
                    obj.close(function () {
                        //create new object (data adapter)
                        newObj = self.createObject();
                        //add createdAt property
                        newObj.createdAt = new Date();
                        //add object hash code property
                        newObj.hashCode = randomHex(HASH_CODE_LENGTH);
                        //delete inUse object
                        delete self.inUse[hashCode];
                        //push object in inUse collection
                        self.inUse[newObj.hashCode] = newObj;
                        //return new object
                        return cb(newObj);
                    });
                }
                else {
                    cb();
                }
            }, function (res) {
                if (res instanceof Error) {
                    callback(res);
                }
                else {
                    callback(null, res);
                }
            });
        }
        else {
            callback();
        }
    }

    /**
     * @private
     * @param {Function} callback
     */
    waitForObject(callback) {
        const self = this;
        let timeout;
        let newObj;
        //register a connection pool timeout
        timeout = setTimeout(function () {
            //throw timeout exception
            const er = new Error('Connection pool timeout.');
            er.code = 'EPTIMEOUT';
            callback(er);
        }, self.options.timeout);
        self.listeners.push(function (releasedObj) {
            //clear timeout
            if (timeout) {
                clearTimeout(timeout);
            }
            if (releasedObj) {
                //push (update) released object in inUse collection
                releasedObj.createdAt = new Date();
                self.inUse[releasedObj.hashCode] = releasedObj;
                //return new object
                return callback(null, releasedObj);
            }
            const keys = _.keys(self.available);
            if (keys.length > 0) {
                const key = keys[0];
                //get connection from available connections
                const pooledObj = self.available[key];
                delete self.available[key];
                //push object in inUse collection
                self.inUse[pooledObj.hashCode] = pooledObj;
                //return pooled object
                callback(null, pooledObj);
            }
            else {
                //create new object
                newObj = self.createObject();
                //add createdAt property
                newObj.createdAt = new Date();
                //add object hash code
                newObj.hashCode = randomHex(HASH_CODE_LENGTH);
                //push object in inUse collection
                self.inUse[newObj.hashCode] = newObj;
                //return new object
                callback(null, newObj);
            }
        });
    }

    /**
     * @private
     * @param {Function} callback
     */
    newObject(callback) {
        const self = this;
        let newObj;
        for (const key in self.available) {
            if (Object.prototype.hasOwnProperty.call(self.available, key)) {
                //get available object
                newObj = self.available[key];
                //delete available key from collection
                delete self.available[key];
                //add createdAt property
                newObj.createdAt = new Date();
                //push object in inUse collection
                self.inUse[newObj.hashCode] = newObj;
                //and finally return it
                return callback(null, newObj);
            }
        }
        //otherwise create new object
        newObj = self.createObject();
        //add createdAt property
        newObj.createdAt = new Date();
        //add object hash code
        newObj.hashCode = randomHex(HASH_CODE_LENGTH);
        //push object in inUse collection
        self.inUse[newObj.hashCode] = newObj;
        //return new object
        return callback(null, newObj);
    }

    /**
     *
     * @param {Function} callback
     */
    getObject(callback) {
        const self = this;
        callback = callback || function () { };

        if (self.state !== 'active') {
            const er = new Error('Connection refused due to pool state.');
            er.code = 'EPSTATE';
            return callback(er);
        }
        const inUseKeys = _.keys(self.inUse);
        debug(`(DataPool): Connections in use: ${inUseKeys.length}`);
        if ((inUseKeys.length < self.options.size) || (self.options.size === 0)) {
            debug("(DataPool): Creating new object in data pool.");
            self.newObject(function (err, result) {
                if (err) { return callback(err); }
                return callback(null, result);
            });
        }
        else {
            self.queryLifetimeForObject(function (err, result) {
                if (err) { return callback(err); }
                if (result) { return callback(null, result); }
                debug("(DataPool): Waiting for an object from data pool.");
                self.waitForObject(function (err, result) {
                    if (err) { return callback(err); }
                    callback(null, result);
                });
            });
        }
    }

    /**
     *
     * @param {*} obj
     * @param {Function} callback
     */
    releaseObject(obj, callback) {
        const self = this;
        callback = callback || function () { };
        if (typeof obj === 'undefined' || obj === null) {
            return callback();
        }
        try {
            //get the first listener
            const listener = self.listeners.shift();
            //if listener exists
            if (typeof listener === 'function') {
                //execute listener
                if (typeof obj.hashCode === 'undefined' || obj.hashCode === null) {
                    //generate hashCode
                    obj.hashCode = randomHex(HASH_CODE_LENGTH);
                }
                if (Object.prototype.hasOwnProperty.call(self.inUse, obj.hashCode)) {
                    //call listener with the released object as parameter
                    listener.call(self, obj);
                }
                else {
                    //validate released object
                    if (typeof obj.close === 'function') {
                        try {
                            //call close() method
                            obj.close();
                            //call listener without any parameter
                            listener.call(self);
                        }
                        catch (e) {
                            log('An error occured while trying to release an unknown data adapter');
                            log(e);
                            //call listener without any parameter
                            listener.call(self);
                        }
                    }
                }
            }
            else {
                //search inUse collection
                debug("(DataPool): Releasing object from data pool.");
                const used = this.inUse[obj.hashCode];
                if (typeof used !== 'undefined') {
                    //delete used adapter
                    delete this.inUse[obj.hashCode];
                    //push data adapter to available collection
                    self.available[used.hashCode] = used;
                    debug(`(DataPool): Object released. Connections in use ${this.inUse.length}.`);
                }
            }
            //finally exit
            callback();
        }
        catch (e) {
            callback(e);
        }
    }
}

/**
 * @class
 */
class PoolAdapter {
    /**
     * @constructor
     * @augments DataAdapter
     * @property {DataAdapter} base
     * @property {DataPool} pool
     */
    constructor(options) {
        this.options = options;
        const self = this;
        Object.defineProperty(this, 'pool', {
            get: function () {
                return DataPool[pools][self.options.pool];
            }, configurable: false, enumerable: false
        });
    }

    /**
     * @param {Function} getConfigurationFunc
     */
    hasConfiguration(getConfigurationFunc) {
        this.pool[getConfigurationMethod] = getConfigurationFunc;
    }

    /**
     * @private
     * @param callback
     */
    open(callback) {
        const self = this;
        if (self.base) {
            return self.base.open(callback);
        }
        else {
            self.pool.getObject(function (err, result) {
                if (err) { return callback(err); }
                self.base = result;
                //add lastIdentity()
                if (self.base && typeof self.base.lastIdentity === 'function') {
                    Object.assign(self, {
                        lastIdentity(callback) {
                            return this.base.lastIdentity(callback);
                        }
                    });
                }
                //add nextIdentity()
                if (self.base && typeof self.base.nextIdentity === 'function') {
                    Object.assign(self, {
                        nextIdentity(entity, attribute, callback) {
                            return this.base.nextIdentity(entity, attribute, callback);
                        }
                    });
                }
                self.base.open(callback);
            });
        }
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
     * @param callback {function(Error=)}
     */
    close(callback) {
        callback = callback || function () { };
        const self = this;
        if (self.base) {
            self.pool.releaseObject(self.base, callback);
            if (typeof self.lastIdentity === 'function') {
                delete self.lastIdentity;
            }
            if (typeof self.nextIdentity === 'function') {
                delete self.nextIdentity;
            }
            delete self.base;
        }
        else {
            callback();
        }
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
     * Executes a query and returns the result as an array of objects.
     * @param query {string|*}
     * @param values {*}
     * @param callback {Function}
     */
    execute(query, values, callback) {
        const self = this;
        self.open(function (err) {
            if (err) { return callback(err); }
            self.base.execute(query, values, callback);
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
     * @param fn {Function} The function to execute
     * @param callback {Function} The callback that contains the error -if any- and the results of the given operation
     */
    executeInTransaction(fn, callback) {
        const self = this;
        self.open(function (err) {
            if (err) { return callback(err); }
            self.base.executeInTransaction(fn, callback);
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
     * @param obj {DataModelMigration|*} An Object that represents the data model scheme we want to migrate
     * @param callback {Function}
     */
    migrate(obj, callback) {
        const self = this;
        self.open(function (err) {
            if (err) { return callback(err); }
            self.base.migrate(obj, callback);
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
 * @param {*} options
 */
function createInstance(options) {
    let name, er;
    if (typeof options.adapter === 'undefined' || options.adapter === null) {
        er = new Error('Invalid argument. The target data adapter is missing.');
        er.code = 'EARG';
        throw er;
    }
    //init pool collection
    DataPool[pools] = DataPool[pools] || {};

    //get adapter's name
    if (typeof options.adapter === 'string') {
        name = options.adapter;
    }
    else if (typeof options.adapter.name === 'string') {
        name = options.adapter.name;
    }
    //validate name
    if (typeof name === 'undefined') {
        er = new Error('Invalid argument. The target data adapter name is missing.');
        er.code = 'EARG';
        throw er;
    }
    /**
     * @type {DataPool}
     */
    let pool = DataPool[pools][name];
    if (pool == null) {
        //create new pool with the name specified in options
        DataPool[pools][name] = new DataPool(options);
    }
    return new PoolAdapter({ pool: name });
}

process.on('exit', function () {
    if (_.isNil(DataPool[pools])) { return; }
    try {
        const keys = _.keys(DataPool[pools]);
        _.forEach(keys, function (x) {
            try {
                log(`Cleaning up data pool (${x})`);
                if (typeof DataPool[pools][x] === 'undefined' || DataPool[pools][x] === null) { return; }
                if (typeof DataPool[pools][x].cleanup === 'function') {
                    DataPool[pools][x].cleanup(function () {
                        //do nothing
                    });
                }
            }
            catch (err) {
                debug(err);
            }
        });
    }
    catch (err) {
        debug(err);
    }

});

module.exports = {
    PoolAdapter,
    PoolDictionary,
    DataPool,
    createInstance
};
