"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/** @module persistence */
/** @hidden */
const _ = require('lodash');
const pip_services3_commons_node_1 = require("pip-services3-commons-node");
const SqlServerPersistence_1 = require("./SqlServerPersistence");
/**
 * Abstract persistence component that stores data in SQLServer
 * and implements a number of CRUD operations over data items with unique ids.
 * The data items must implement IIdentifiable interface.
 *
 * In basic scenarios child classes shall only override [[getPageByFilter]],
 * [[getListByFilter]] or [[deleteByFilter]] operations with specific filter function.
 * All other operations can be used out of the box.
 *
 * In complex scenarios child classes can implement additional operations by
 * accessing <code>this._collection</code> and <code>this._model</code> properties.

 * ### Configuration parameters ###
 *
 * - collection:                  (optional) SQLServer collection name
 * - connection(s):
 *   - discovery_key:             (optional) a key to retrieve the connection from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]]
 *   - host:                      host name or IP address
 *   - port:                      port number (default: 27017)
 *   - uri:                       resource URI or connection string with all parameters in it
 * - credential(s):
 *   - store_key:                 (optional) a key to retrieve the credentials from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/auth.icredentialstore.html ICredentialStore]]
 *   - username:                  (optional) user name
 *   - password:                  (optional) user password
 * - options:
 *   - connect_timeout:      (optional) number of milliseconds to wait before timing out when connecting a new client (default: 0)
 *   - idle_timeout:         (optional) number of milliseconds a client must sit idle in the pool and not be checked out (default: 10000)
 *   - max_pool_size:        (optional) maximum number of clients the pool should contain (default: 10)
 *
 * ### References ###
 *
 * - <code>\*:logger:\*:\*:1.0</code>           (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/log.ilogger.html ILogger]] components to pass log messages components to pass log messages
 * - <code>\*:discovery:\*:\*:1.0</code>        (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services
 * - <code>\*:credential-store:\*:\*:1.0</code> (optional) Credential stores to resolve credentials
 *
 * ### Example ###
 *
 *     class MySqlServerPersistence extends IdentifiableSqlServerPersistence<MyData, string> {
 *
 *     public constructor() {
 *         base("mydata", new MyDataSqlServerSchema());
 *     }
 *
 *     private composeFilter(filter: FilterParams): any {
 *         filter = filter || new FilterParams();
 *         let criteria = [];
 *         let name = filter.getAsNullableString('name');
 *         if (name != null)
 *             criteria.push({ name: name });
 *         return criteria.length > 0 ? { $and: criteria } : null;
 *     }
 *
 *     public getPageByFilter(correlationId: string, filter: FilterParams, paging: PagingParams,
 *         callback: (err: any, page: DataPage<MyData>) => void): void {
 *         base.getPageByFilter(correlationId, this.composeFilter(filter), paging, null, null, callback);
 *     }
 *
 *     }
 *
 *     let persistence = new MySqlServerPersistence();
 *     persistence.configure(ConfigParams.fromTuples(
 *         "host", "localhost",
 *         "port", 27017
 *     ));
 *
 *     persitence.open("123", (err) => {
 *         ...
 *     });
 *
 *     persistence.create("123", { id: "1", name: "ABC" }, (err, item) => {
 *         persistence.getPageByFilter(
 *             "123",
 *             FilterParams.fromTuples("name", "ABC"),
 *             null,
 *             (err, page) => {
 *                 console.log(page.data);          // Result: { id: "1", name: "ABC" }
 *
 *                 persistence.deleteById("123", "1", (err, item) => {
 *                    ...
 *                 });
 *             }
 *         )
 *     });
 */
class IdentifiableSqlServerPersistence extends SqlServerPersistence_1.SqlServerPersistence {
    /**
     * Creates a new instance of the persistence component.
     *
     * @param collection    (optional) a collection name.
     */
    constructor(tableName) {
        super(tableName);
        if (tableName == null)
            throw new Error("Table name could not be null");
    }
    /**
     * Converts the given object from the public partial format.
     *
     * @param value     the object to convert from the public partial format.
     * @returns the initial object.
     */
    convertFromPublicPartial(value) {
        return this.convertFromPublic(value);
    }
    /**
     * Gets a list of data items retrieved by given unique ids.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param ids               ids of data items to be retrieved
     * @param callback         callback function that receives a data list or error.
     */
    getListByIds(correlationId, ids, callback) {
        let params = this.generateParameters(ids);
        let query = "SELECT * FROM " + this.quoteIdentifier(this._tableName) + " WHERE [id] IN(" + params + ")";
        let request = this.createRequest(ids);
        request.query(query, (err, result) => {
            err = err || null;
            if (err) {
                callback(err, null);
                return;
            }
            let items = result.recordset;
            if (items != null)
                this._logger.trace(correlationId, "Retrieved %d from %s", items.length, this._tableName);
            items = _.map(items, this.convertToPublic);
            callback(null, items);
        });
    }
    /**
     * Gets a data item by its unique id.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param id                an id of data item to be retrieved.
     * @param callback          callback function that receives data item or error.
     */
    getOneById(correlationId, id, callback) {
        let query = "SELECT * FROM " + this.quoteIdentifier(this._tableName) + " WHERE [id]=@1";
        let params = [id];
        let request = this.createRequest(params);
        request.query(query, (err, result) => {
            err = err || null;
            let item = result && result.recordset ? result.recordset[0] || null : null;
            if (item == null)
                this._logger.trace(correlationId, "Nothing found from %s with id = %s", this._tableName, id);
            else
                this._logger.trace(correlationId, "Retrieved from %s with id = %s", this._tableName, id);
            item = this.convertToPublic(item);
            callback(err, item);
        });
    }
    /**
     * Creates a data item.
     *
     * @param correlation_id    (optional) transaction id to trace execution through call chain.
     * @param item              an item to be created.
     * @param callback          (optional) callback function that receives created item or error.
     */
    create(correlationId, item, callback) {
        if (item == null) {
            callback(null, null);
            return;
        }
        // Assign unique id
        let newItem = item;
        if (newItem.id == null) {
            newItem = _.clone(newItem);
            newItem.id = item.id || pip_services3_commons_node_1.IdGenerator.nextLong();
        }
        super.create(correlationId, newItem, callback);
    }
    /**
     * Sets a data item. If the data item exists it updates it,
     * otherwise it create a new data item.
     *
     * @param correlation_id    (optional) transaction id to trace execution through call chain.
     * @param item              a item to be set.
     * @param callback          (optional) callback function that receives updated item or error.
     */
    set(correlationId, item, callback) {
        if (item == null) {
            if (callback)
                callback(null, null);
            return;
        }
        // Assign unique id
        if (item.id == null) {
            item = _.clone(item);
            item.id = pip_services3_commons_node_1.IdGenerator.nextLong();
        }
        let row = this.convertFromPublic(item);
        let columns = this.generateColumns(row);
        let params = this.generateParameters(row);
        let setParams = this.generateSetParameters(row);
        let values = this.generateValues(row);
        values.push(item.id);
        let query = "INSERT INTO " + this.quoteIdentifier(this._tableName) + " (" + columns + ") OUTPUT INSERTED.* VALUES (" + params + ")";
        let request = this.createRequest(values);
        request.query(query, (err, result) => {
            err = err || null;
            // Suppress duplicated error entry
            if (err && (err.number == 2601 || err.number == 2627)) {
                err = null;
                result == null;
            }
            let newItem = result && result.recordset && result.recordset.length == 1
                ? this.convertToPublic(result.recordset[0]) : null;
            if (newItem != null || err != null) {
                if (!err)
                    this._logger.trace(correlationId, "Set in %s with id = %s", this._tableName, item.id);
                if (callback)
                    callback(err, newItem);
                return;
            }
            values.push(item.id);
            let query = "UPDATE " + this.quoteIdentifier(this._tableName) + " SET " + setParams + " OUTPUT INSERTED.* WHERE [id]=@" + values.length;
            let request = this.createRequest(values);
            request.query(query, (err, result) => {
                err = err || null;
                if (!err)
                    this._logger.trace(correlationId, "Set in %s with id = %s", this._tableName, item.id);
                let newItem = result && result.recordset && result.recordset.length == 1
                    ? this.convertToPublic(result.recordset[0]) : null;
                if (callback)
                    callback(err, newItem);
            });
        });
    }
    /**
     * Updates a data item.
     *
     * @param correlation_id    (optional) transaction id to trace execution through call chain.
     * @param item              an item to be updated.
     * @param callback          (optional) callback function that receives updated item or error.
     */
    update(correlationId, item, callback) {
        if (item == null || item.id == null) {
            if (callback)
                callback(null, null);
            return;
        }
        let row = this.convertFromPublic(item);
        let params = this.generateSetParameters(row);
        let values = this.generateValues(row);
        values.push(item.id);
        let query = "UPDATE " + this.quoteIdentifier(this._tableName)
            + " SET " + params + " OUTPUT INSERTED.* WHERE [id]=@" + values.length;
        let request = this.createRequest(values);
        request.query(query, (err, result) => {
            err = err || null;
            if (!err)
                this._logger.trace(correlationId, "Updated in %s with id = %s", this._tableName, item.id);
            let newItem = result && result.recordset && result.recordset.length == 1
                ? this.convertToPublic(result.recordset[0]) : null;
            if (callback)
                callback(err, newItem);
        });
    }
    /**
     * Updates only few selected fields in a data item.
     *
     * @param correlation_id    (optional) transaction id to trace execution through call chain.
     * @param id                an id of data item to be updated.
     * @param data              a map with fields to be updated.
     * @param callback          callback function that receives updated item or error.
     */
    updatePartially(correlationId, id, data, callback) {
        if (data == null || id == null) {
            if (callback)
                callback(null, null);
            return;
        }
        let row = this.convertFromPublicPartial(data.getAsObject());
        let params = this.generateSetParameters(row);
        let values = this.generateValues(row);
        values.push(id);
        let query = "UPDATE " + this.quoteIdentifier(this._tableName)
            + " SET " + params + " OUTPUT INSERTED.* WHERE [id]=@" + values.length;
        let request = this.createRequest(values);
        request.query(query, (err, result) => {
            err = err || null;
            if (!err)
                this._logger.trace(correlationId, "Updated partially in %s with id = %s", this._tableName, id);
            let newItem = result && result.recordset && result.recordset.length == 1
                ? this.convertToPublic(result.recordset[0]) : null;
            if (callback)
                callback(err, newItem);
        });
    }
    /**
     * Deleted a data item by it's unique id.
     *
     * @param correlation_id    (optional) transaction id to trace execution through call chain.
     * @param id                an id of the item to be deleted
     * @param callback          (optional) callback function that receives deleted item or error.
     */
    deleteById(correlationId, id, callback) {
        let values = [id];
        let query = "DELETE FROM " + this.quoteIdentifier(this._tableName) + " OUTPUT DELETED.* WHERE [id]=@1";
        let request = this.createRequest(values);
        request.query(query, (err, result) => {
            err = err || null;
            if (!err)
                this._logger.trace(correlationId, "Deleted from %s with id = %s", this._tableName, id);
            let newItem = result && result.recordset && result.recordset.length == 1
                ? this.convertToPublic(result.recordset[0]) : null;
            if (callback)
                callback(err, newItem);
        });
    }
    /**
     * Deletes multiple data items by their unique ids.
     *
     * @param correlationId     (optional) transaction id to trace execution through call chain.
     * @param ids               ids of data items to be deleted.
     * @param callback          (optional) callback function that receives error or null for success.
     */
    deleteByIds(correlationId, ids, callback) {
        let params = this.generateParameters(ids);
        let query = "DELETE FROM " + this.quoteIdentifier(this._tableName) + " WHERE \"id\" IN(" + params + ")";
        let request = this.createRequest(ids);
        request.query(query, (err, result) => {
            let count = result && result.rowsAffected ? result.rowsAffected[0] : 0;
            err = err || null;
            if (!err)
                this._logger.trace(correlationId, "Deleted %d items from %s", count, this._tableName);
            if (callback)
                callback(err);
        });
    }
}
exports.IdentifiableSqlServerPersistence = IdentifiableSqlServerPersistence;
//# sourceMappingURL=IdentifiableSqlServerPersistence.js.map