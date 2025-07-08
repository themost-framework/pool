const {ApplicationService} = require('@themost/common');
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
                promClient.register.registerMetric(exporter);
            }
        });
    }

}

module.exports = {
    GenericPoolAdapterExporter
}