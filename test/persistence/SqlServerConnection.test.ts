const assert = require('chai').assert;
const process = require('process');

import { ConfigParams } from 'pip-services3-commons-node';
import { SqlServerConnection } from '../../src/persistence/SqlServerConnection';

suite('SqlServerConnection', ()=> {
    let connection: SqlServerConnection;

    let sqlserverUri = process.env['SQLSERVER_URI'];
    let sqlserverHost = process.env['SQLSERVER_HOST'] || 'localhost';
    let sqlserverPort = process.env['SQLSERVER_PORT'] || 1433;
    let sqlserverDatabase = process.env['SQLSERVER_DB'] || 'master';
    let sqlserverUser = process.env['SQLSERVER_USER'] || 'sa';
    let sqlserverPassword = process.env['SQLSERVER_PASSWORD'] || 'sqlserver_123';
    if (sqlserverUri == null && sqlserverHost == null)
        return;

    setup((done) => {
        let dbConfig = ConfigParams.fromTuples(
            'connection.uri', sqlserverUri,
            'connection.host', sqlserverHost,
            'connection.port', sqlserverPort,
            'connection.database', sqlserverDatabase,
            'credential.username', sqlserverUser,
            'credential.password', sqlserverPassword
        );

        connection = new SqlServerConnection();
        connection.configure(dbConfig);

        connection.open(null, done);
    });

    teardown((done) => {
        connection.close(null, done);
    });

    test('Open and Close', (done) => {
        assert.isObject(connection.getConnection());
        assert.isString(connection.getDatabaseName());

        done();
    });
});