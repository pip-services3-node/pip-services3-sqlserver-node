/** @module persistence */
const _ = require('lodash');

import { request } from 'http';
import { IReferenceable } from 'pip-services3-commons-node';
import { IReferences } from 'pip-services3-commons-node';
import { IConfigurable } from 'pip-services3-commons-node';
import { IOpenable } from 'pip-services3-commons-node';
import { ConfigParams } from 'pip-services3-commons-node';
import { ConnectionException } from 'pip-services3-commons-node';
import { CompositeLogger } from 'pip-services3-components-node';

import { SqlServerConnectionResolver } from '../connect/SqlServerConnectionResolver';

/**
 * SQLServer connection using plain driver.
 * 
 * By defining a connection and sharing it through multiple persistence components
 * you can reduce number of used database connections.
 * 
 * ### Configuration parameters ###
 * 
 * - connection(s):    
 *   - discovery_key:             (optional) a key to retrieve the connection from [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/connect.idiscovery.html IDiscovery]]
 *   - host:                      host name or IP address
 *   - port:                      port number (default: 27017)
 *   - uri:                       resource URI or connection string with all parameters in it
 * - credential(s):    
 *   - store_key:                 (optional) a key to retrieve the credentials from [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/auth.icredentialstore.html ICredentialStore]]
 *   - username:                  user name
 *   - password:                  user password
 * - options:
 *   - connect_timeout:      (optional) number of milliseconds to wait before timing out when connecting a new client (default: 0)
 *   - idle_timeout:         (optional) number of milliseconds a client must sit idle in the pool and not be checked out (default: 10000)
 *   - max_pool_size:        (optional) maximum number of clients the pool should contain (default: 10)
 * 
 * ### References ###
 * 
 * - <code>\*:logger:\*:\*:1.0</code>           (optional) [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/log.ilogger.html ILogger]] components to pass log messages
 * - <code>\*:discovery:\*:\*:1.0</code>        (optional) [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/connect.idiscovery.html IDiscovery]] services
 * - <code>\*:credential-store:\*:\*:1.0</code> (optional) Credential stores to resolve credentials
 * 
 */
export class SqlServerConnection implements IReferenceable, IConfigurable, IOpenable {

    private _defaultConfig: ConfigParams = ConfigParams.fromTuples(
        // connections.*
        // credential.*

        "options.connect_timeout", 15000,
        "options.request_timeout", 15000,
        "options.idle_timeout", 30000,
        "options.max_pool_size", 3
    );

    /** 
     * The logger.
     */
    protected _logger: CompositeLogger = new CompositeLogger();
    /**
     * The connection resolver.
     */
    protected _connectionResolver: SqlServerConnectionResolver = new SqlServerConnectionResolver();
    /**
     * The configuration options.
     */
    protected _options: ConfigParams = new ConfigParams();

    /**
     * The SQLServer connection pool object.
     */
    protected _connection: any;
    /**
     * The SQLServer database name.
     */
    protected _databaseName: string;

    /**
     * Creates a new instance of the connection component.
     */
    public constructor() {}

    /**
     * Configures component by passing configuration parameters.
     * 
     * @param config    configuration parameters to be set.
     */
    public configure(config: ConfigParams): void {
        config = config.setDefaults(this._defaultConfig);

        this._connectionResolver.configure(config);

        this._options = this._options.override(config.getSection("options"));
    }

    /**
	 * Sets references to dependent components.
	 * 
	 * @param references 	references to locate the component dependencies. 
     */
    public setReferences(references: IReferences): void {
        this._logger.setReferences(references);
        this._connectionResolver.setReferences(references);
    }

    /**
	 * Checks if the component is opened.
	 * 
	 * @returns true if the component has been opened and false otherwise.
     */
    public isOpen(): boolean {
        return this._connection != null;
    }

    private composeUriSettings(uri: string): string {
        let maxPoolSize = this._options.getAsNullableInteger("max_pool_size");
        let connectTimeoutMS = this._options.getAsNullableInteger("connect_timeout");
        let requestTimeoutMS = this._options.getAsNullableInteger("request_timeout");
        let idleTimeoutMS = this._options.getAsNullableInteger("idle_timeout");

        let settings: any = {
            // parseJSON: true,
            // connectTimeout: connectTimeoutMS,
            // requestTimeout: requestTimeoutMS,
            // 'pool.min': 0,
            // 'pool.max': maxPoolSize,
            // 'pool.idleTimeoutMillis': idleTimeoutMS
        };

        let params = '';
        for (let key in settings) {
            if (params.length > 0)
                params += '&';

            params += key;

            let value = settings[key];
            if (value != null)
                params += '=' + value;
        }
        if (uri.indexOf('?') < 0)
            uri += '?' + params;
        else uri += '&' + params;

        return uri;
    }

    /**
	 * Opens the component.
	 * 
	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    public open(correlationId: string, callback?: (err: any) => void): void {
        this._connectionResolver.resolve(correlationId, (err, uri) => {
            if (err) {
                if (callback) callback(err);
                else this._logger.error(correlationId, err, 'Failed to resolve SqlServer connection');
                return;
            }

            this._logger.debug(correlationId, "Connecting to sqlserver");

            try {
                uri = this.composeUriSettings(uri);

                const sql = require('mssql')
                const pool = new sql.ConnectionPool(uri);
                pool.config.options.enableArithAbort = true;

                // Try to connect
                pool.connect((err) => {
                    if (err != null) {
                        err = new ConnectionException(correlationId, "CONNECT_FAILED", "Connection to sqlserver failed").withCause(err);
                    } else {
                        this._connection = pool;                        
                        this._databaseName = pool.config.database;
                    }

                    if (callback) callback(err);
                });
            } catch (ex) {
                let err = new ConnectionException(correlationId, "CONNECT_FAILED", "Connection to sqlserver failed").withCause(ex);

                callback(err);
            }
        });
    }

    /**
	 * Closes component and frees used resources.
	 * 
	 * @param correlationId 	(optional) transaction id to trace execution through call chain.
     * @param callback 			callback function that receives error or null no errors occured.
     */
    public close(correlationId: string, callback?: (err: any) => void): void {
        if (this._connection == null) {
            if (callback) callback(null);
            return;
        }

        this._connection.close((err) => {
            if (err)
                err = new ConnectionException(correlationId, 'DISCONNECT_FAILED', 'Disconnect from sqlserver failed: ') .withCause(err);
            else
                this._logger.debug(correlationId, "Disconnected from sqlserver database %s", this._databaseName);

            this._connection = null;
            this._databaseName = null;
    
            if (callback) callback(err);
        });
    }

    public getConnection(): any {
        return this._connection;
    }

    public getDatabaseName(): string {
        return this._databaseName;
    }

}
