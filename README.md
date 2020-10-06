# themost-pool
Most Web Framework data adapter for connection pooling

## Install
    npm install @themost/pool
## Usage
Register Generic Pool Adapter on app.json as follows:

    "adapterTypes": [
        ...
        { "name":"...", "invariantName": "...", "type":"..." },
        { "name":"Pool Data Adapter", "invariantName": "pool", "type":"@themost/pool" }
        ...
    ],
    adapters: [
        ...
        { "name":"development", "invariantName":"...", "default":false,
            "options": {
              "server":"localhost",
              "user":"user",
              "password":"password",
              "database":"test"
            }
        },
        { "name":"development_with_pool", "invariantName":"pool", "default":true,
                    "options": {
                      "adapter":"development"
                    }
                }
        ...
    ]

The generic pool adapter will try to instantiate the adapter defined in options.adapter property.

# Options

### adapter:
The name of the data adapter to be linked with this pool adapter.

`@themost/pool` adapter uses [generic-pool](https://github.com/coopernurse/node-pool#documentation). 
Read more about `generic-pool` [here](https://github.com/coopernurse/node-pool#documentation)

Important Note: Upgrade from 2.2.x to 2.5.x

Replace `@themost/pool@2.2.x` configuration:

    {
        "adapter": "development",
        "size": 25,
        "timeout": 30000,
        "lifetime": 1200000
    }

with:

    {
        "adapter": "development",
        "max": 25,
        "acquireTimeoutMillis": 30000
    }
