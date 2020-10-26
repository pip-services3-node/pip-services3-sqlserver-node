const process = require('process');
const assert = require('chai').assert;

import { ConfigParams } from 'pip-services3-commons-node';
import { Descriptor } from 'pip-services3-commons-node';
import { References } from 'pip-services3-commons-node';
import { SqlServerConnection } from '../../src/persistence/SqlServerConnection';
import { DummyPersistenceFixture } from '../fixtures/DummyPersistenceFixture';
import { DummySqlServerPersistence } from './DummySqlServerPersistence';

suite('DummySqlServerConnection', ()=> {
    let connection: SqlServerConnection;
    let persistence: DummySqlServerPersistence;
    let fixture: DummyPersistenceFixture;

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

        persistence = new DummySqlServerPersistence();
        persistence.setReferences(References.fromTuples(
            new Descriptor("pip-services", "connection", "sqlserver", "default", "1.0"), connection
        ));

        fixture = new DummyPersistenceFixture(persistence);

        connection.open(null, (err: any) => {
            if (err) {
                done(err);
                return;
            }

            persistence.open(null, (err: any) => {
                if (err) {
                    done(err);
                    return;
                }
    
                persistence.clear(null, (err) => {
                    done(err);
                });
            });
        });
    });

    teardown((done) => {
        connection.close(null, (err) => {
            persistence.close(null, done);
        });
    });

    test('Connection', (done) => {
        assert.isObject(connection.getConnection());
        assert.isString(connection.getDatabaseName());

        done();
    });

    test('Crud Operations', (done) => {
        fixture.testCrudOperations(done);
    });

    test('Batch Operations', (done) => {
        fixture.testBatchOperations(done);
    });
});