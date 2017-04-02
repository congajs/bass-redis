/*
 * This file is part of the bass-redis library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

const path = require('path');

const BassConnection = require('../../bass').Connection

module.exports = class Connection extends BassConnection {

	constructor(client, database, logger) {
		super(client, logger);
		this.database = database;
	}

	/**
	 * 
	 * @param  {Metadata} metadata
	 * @param  {Function} cb
	 * @return {void}
	 */
	boot(metadataRegistry, cb) {

		cb(null);
	}

}
