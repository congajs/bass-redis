/*
 * This file is part of the bass-redis library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
const redis = require('redis');

const Connection = require('./connection');

module.exports = class ConnectionFactory {

    static factory(config, logger, cb) {

        const client = redis.createClient({
            host: config.host,
            port: config.port
        });

        client.on("error", function(err) {
            cb(err);
        });

        client.on("ready", function(err) {
            const connection = new Connection(client, config.database, logger);
            cb(null, connection);
        });

    }
}
