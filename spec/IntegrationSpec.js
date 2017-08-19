
const redis = require('redis');
const AdapterIntegration = require('../node_modules/bass/spec/AdapterIntegration');

const client = redis.createClient({
    host: 'localhost',
    port: '6379'
});

client.on("ready", function(err) {

    client.flushall(() => {

        console.log('flushed');

    });

});

describe('bass-redis', AdapterIntegration('bass-redis', {
    connections: {
        default: {
            adapter: 'bass-redis',
            host: 'localhost',
            port: '6379',
            database: 'bass-redis-test'
        }
    }
}));
