const AdapterIntegrationSpec = require('../node_modules/bass/spec/AdapterIntegrationSpec');

describe('bass-redis', AdapterIntegrationSpec('bass-redis', {
    connections: {
        default: {
            adapter: 'bass-redis',
            host: 'localhost',
            port: '6379',
            database: 'test'
        }
    }
}));
