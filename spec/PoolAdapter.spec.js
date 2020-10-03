const { createInstance } = require('../index');

describe('PoolAdapter', () => {
    it('should create instance', () => {
        const adapter = createInstance(
            {
                "adapter": "development"
            });
        expect(adapter).toBeTruthy();
    });
});