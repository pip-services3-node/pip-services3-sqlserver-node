const assert = require('chai').assert;

import { ConfigParams } from 'pip-services3-commons-node';
import { SqlServerConnectionResolver } from '../../src/connect/SqlServerConnectionResolver';

suite('SqlServerConnectionResolver', ()=> {

    test('Connection Config', (done) => {
        let dbConfig = ConfigParams.fromTuples(
            'connection.host', 'localhost',
            'connection.port', 1433,
            'connection.database', 'test',
            'connection.encrypt', true,
            'credential.username', 'sa',
            'credential.password', 'pwd#123',
        );

        let resolver = new SqlServerConnectionResolver();
        resolver.configure(dbConfig);

        resolver.resolve(null, (err, uri) => {
            assert.isNull(err);

            assert.isString(uri);
            assert.equal('mssql://sa:pwd#123@localhost:1433/test?encrypt=true', uri);

            done(err);
        });
    });
});