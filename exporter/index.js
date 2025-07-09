const {ApplicationService, TraceUtils} = require('@themost/common');
// eslint-disable-next-line node/no-extraneous-require
const {GenericPoolAdapter} = require('@themost/pool');
const promClient = require('prom-client');
const poolExporter = require('generic-pool-prometheus-exporter');

class GenericPoolAdapterExporter extends ApplicationService {
    constructor(app) {
        super(app);
        this.register();
    }

    register() {
        GenericPoolAdapter.created.subscribe(async ({target}) => {
            const exporter = poolExporter(target);
            if (exporter) {
                // register exporter with prom-client
                promClient.register.metrics().then(() => {
                    TraceUtils.info(`Registered generic pool exporter for ${target.name}`);
                }).catch((err) => {
                    TraceUtils.error(`Failed to register generic pool exporter for ${target.name}:`, err);
                });
            }   
        });
    }

}

module.exports = {
    GenericPoolAdapterExporter
}