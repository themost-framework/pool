const { JsonLogger } = require('@themost/json-logger');
const { TraceUtils } = require('@themost/common');
// use dotenv
require('dotenv').config()
// use json logger
TraceUtils.useLogger(new JsonLogger({
    format: 'raw'
}));
