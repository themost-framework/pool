const { createInstance, GenericPoolAdapter } = require('../index');
const { ConfigurationBase } = require('@themost/common');
const sqlite = require('@themost/sqlite');
const { QueryExpression } = require('@themost/query');

const Products = require('./config/models/Product.json');

class TestDataConfiguationStrategy {

    constructor() {
        this.adapters = [
            {
                "name": "test", "invariantName": "sqlite",
                "options": {
                    "database": "spec/db/test.db"
                }
            }
        ]
        this.adapterTypes = [
            {
                invariantName: 'sqlite',
                adapterType: sqlite
            }
        ]

    }

    getAdapterType(invariantName) {
        const find = this.adapterTypes.find((x) => {
            return x.invariantName === invariantName;
        });
        if (find == null) {
            return;
        }
        return find.adapterType;
    }
}

describe('PoolAdapter', () => {
    let configuration;
    beforeAll(() => {
        configuration = new ConfigurationBase();
        configuration.useStrategy(function DataConfigurationStrategy() {
        }, TestDataConfiguationStrategy);
    })
    it('should create instance', () => {
        const adapter = createInstance(
            {
                "adapter": "test",
                "size": 20
            });
        adapter.hasConfiguration(() => {
            return configuration;
        });
        expect(adapter).toBeTruthy();
        const pool = GenericPoolAdapter.pool('test');
        expect(pool).toBeTruthy();
        expect(pool.max).toBe(20);
    });

    it('should open and close', async () => {
        const adapter = createInstance(
            {
                "adapter": "test"
            });
        adapter.hasConfiguration(() => {
            return configuration;
        });
        // open
        await adapter.openAsync();
        expect(adapter.base).toBeTruthy();
        // validate pool property
        expect(adapter.base.pool).toBeTruthy();
        expect(adapter.base.pool).toBe('test');
        // close
        await adapter.closeAsync();
        expect(adapter.base).toBeFalsy();
    });

    it('should get connection from pool', async () => {
        const adapter = createInstance(
            {
                "adapter": "test"
            });
        adapter.hasConfiguration(() => {
            return configuration;
        });
        // open
        await adapter.openAsync();
        expect(adapter.base).toBeTruthy();

        const pool = GenericPoolAdapter.pool(adapter.base.pool);
        expect(pool).toBeTruthy();
        expect(pool.acquire).toBeInstanceOf(Function);
        const newAdapter = await pool.acquire();
        expect(newAdapter).toBeTruthy();
        // execute a query
        await newAdapter.migrateAsync({
            appliesTo: Products.source,
            add: Products.fields,
            version: Products.version
        });
        const query = new QueryExpression().from('Products')
            .select('ProductID', 'ProductName')
        const items = await newAdapter.executeAsync(query, null);
        expect(items).toBeTruthy();
        await pool.release(newAdapter);
        // close
        await adapter.closeAsync();
        expect(adapter.base).toBeFalsy();
    });

    it('should create table', async () => {
        const adapter = createInstance(
            {
                "adapter": "test"
            });
        adapter.hasConfiguration(() => {
            return configuration;
        });

        await adapter.migrateAsync({
            appliesTo: Products.source,
            add: Products.fields,
            version: Products.version
        });

        const query = new QueryExpression().from('Products')
            .select('ProductID', 'ProductName')
        const items = await adapter.executeAsync(query, null);

        expect(items).toBeInstanceOf(Array);

        expect(items.length).toBe(0);

    });

    it('should execute query', async () => {
        const adapter = createInstance(
            {
                "adapter": "test"
            });
        adapter.hasConfiguration(() => {
            return configuration;
        });

        await adapter.migrateAsync({
            appliesTo: Products.source,
            add: Products.fields,
            version: Products.version
        });
        let query = new QueryExpression().from('Products')
            .select('ProductID', 'ProductName');
        let items = await adapter.executeAsync(query, null);
        expect(items).toBeInstanceOf(Array);
        query = new QueryExpression().insert({
            "ProductID": 1,
            "ProductName": "Chais",
            "SupplierID": 1,
            "CategoryID": 1,
            "Unit": "10 boxes x 20 bags",
            "Price": 18.0
        }).into('Products');
        await adapter.executeAsync(query, null);

        query = new QueryExpression().from('Products')
            .select('ProductID', 'ProductName')
            .where('ProductID').equal(1);
        items = await adapter.executeAsync(query, null);
        expect(items).toBeInstanceOf(Array);
        expect(items.length).toBeGreaterThan(0);

        query = new QueryExpression().delete('Products')
            .where('ProductID').equal(1);
        await adapter.executeAsync(query, null);

    });

});