# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> SQLServer components for Node.js Changelog

## <a name="3.4.0"></a> 3.4.0 (2020-06-09) 

### Features
* Moved some CRUD operations from IdentifiableSqlServerPersistence to SqlServerPersistence

## <a name="3.3.0"></a> 3.3.0 (2020-05-18) 

### Features
* Added getCountByFilter

## <a name="3.2.6"></a> 3.2.6 (2020-04-10) 

### Bug fixes
* Fixed logging message of the collection name in IdentifiableSqlServerPersistence

## <a name="3.2.0"></a> 3.2.0 (2019-11-05) 

### Features
* Added SqlServerConnection
* Added SqlServerPersistence.ensureIndex()

## <a name="3.1.0"></a> 3.1.0 (2019-05-20) 

### Breaking Changes
* Reimplemented persistence using plain SQLServer driver
* Mongoose-based persistence moved to a separate package

## <a name="3.0.0"></a> 3.0.0 (2018-08-21) 

### Breaking Changes
* Moved to a separate package

## <a name="1.2.0"></a> 1.2.0 (2018-08-10) 

### Features
* Added RedisCache
* Added RedisLock

## <a name="1.1.0"></a> 1.1.0 (2018-03-26) 

### Features
* Added PrometheusCounters and PrometheusMetricsService
* Added labels to PrometheusCounters and PrometheusMetricsService

## <a name="1.0.0"></a> 1.0.0 (2018-03-20) 

### Features
* **memcached** Added MemcachedCache
* **memcached** Added MemcachedLock
* **fluentd** Added FluentdLogger
* **elasticsearch** Added ElasticSearchLogger

