const { createInstance, GenericPoolAdapter } = require('../index');
const { getApplication } = require('@themost/test');
const { DataConfigurationStrategy, DataCacheStrategy} = require('@themost/data');
require('@themost/promise-sequence');

describe('GenericPoolAdapter', () => {
    /**
     * @type {import('@themost/express').ExpressDataContext}
     */
    let context;
    beforeAll(() => {
        const container = getApplication();
        /**
         * @type {import('@themost/express').ExpressDataApplication}
         */
        const app = container.get('ExpressDataApplication');
        const conf = app.getConfiguration().getStrategy(DataConfigurationStrategy);
        conf.adapters.unshift({
            default: true,
            name: 'test+pool',
            invariantName: 'pool',
            options: {
                adapter: 'test',
                size: 20
            }
        });
        Object.assign(conf.adapterTypes, {
            pool: {
                invariantName: 'pool',
                name: 'Connection pooling adapter',
                createInstance: (options) => {
                    return createInstance(options);
                }
            }
        });
        context = app.createContext();
    });
    afterAll(async () => {
        await context.finalizeAsync();
        /**
         * @type {DataCacheStrategy|*}
         */
        const service = context.application.getConfiguration().getStrategy(DataCacheStrategy);
        if (typeof service.finalize === 'function') {
            await service.finalize();
        }
    });
    it('should create instance', () => {
        const db = context.db;
        expect(db instanceof GenericPoolAdapter).toBeTruthy();
    });

    it('should execute query', async () => {
        const items = await context.model('Product').where(
            (x) => x.category === 'Laptops'
        ).getItems();
        expect(items).toBeTruthy();
        /**
         * @type {GenericPoolAdapter|*}
         */
        const db = context.db;
        expect(db.base).toBeFalsy();
    });

    it('should execute queries in sequence', async () => {
        const [laptops, smartphones] = await Promise.sequence([
            () => context.model('Product').where(
                (x) => x.category === 'Laptops'
            ).getItems(),
            () => context.model('Product').where(
                (x) => x.category === 'Smartphones'
            ).getItems()
        ]);
        expect(laptops).toBeTruthy();
        expect(smartphones).toBeTruthy();
        /**
         * @type {GenericPoolAdapter|*}
         */
        const db = context.db;
        expect(db.base).toBeFalsy();
    });

    it('should execute queries in parallel', async () => {
        const [laptops, desktops] = await Promise.all([
            context.model('Product').where(
                (x) => x.category === 'Laptops'
            ).getItems(),
            context.model('Product').where(
                (x) => x.category === 'Desktops'
            ).getItems()
        ]);
        expect(laptops).toBeTruthy();
        expect(desktops).toBeTruthy();
        /**
         * @type {GenericPoolAdapter|*}
         */
        const db = context.db;
        expect(db.base).toBeFalsy();
    });

    it('should execute with error', async () => {
        // noinspection JSUnresolvedReference
        await expect(context.model('Product').where(
            (x) => x.otherCategory === 'Laptops'
        ).getItems()).rejects.toBeTruthy();
        /**
         * @type {GenericPoolAdapter|*}
         */
        const db = context.db;
        expect(db.base).toBeFalsy();
    });

    it('should use executeInTransaction', async () => {
        await context.db.executeInTransactionAsync(async () => {
            const items = await context.model('Product').where(
                (x) => x.category === 'Laptops'
            ).getItems();
            expect(items).toBeTruthy();
        });
        /**
         * @type {GenericPoolAdapter|*}
         */
        const db = context.db;
        expect(db.base).toBeFalsy();
    });

    it('should use executeInTransaction with error', async () => {
        await expect(context.db.executeInTransactionAsync(async () => {
            // noinspection JSUnresolvedReference
            await context.model('Product').where(
                (x) => x.category === 'Laptops'
            ).getItems();
            throw new Error('Test error');
        })).rejects.toBeTruthy();
        /**
         * @type {GenericPoolAdapter|*}
         */
        const db = context.db;
        expect(db.base).toBeFalsy();
    });

    it('should use method from base connection', async () => {
        await context.db.openAsync();
        const columns = await context.db.table('ProductBase').columnsAsync();
        expect(columns).toBeTruthy();
        expect(columns.length).toBeTruthy();
    });

});
