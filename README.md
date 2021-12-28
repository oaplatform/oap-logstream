# Logstream

## Logger
Generic logger

## MapLogger
Logging generic maps (usually deserialized JSONs)
Extend MapLogger for usage

## DynamicMapLogger
For universal map logging (deserialized JSONs of multiple types)
Implement DynamicMapLogger.Extractor and link to DynamicMapLogger for usage

## ObjectLogger
Effective oap.template based object logger
Extend ObjectLogger for usage

## DataModel
 
[DataModel.java](oap-logstream-data/src/main/java/oap/logstream/data/DataModel.java) 

Map data objects to tsv/csv/json files. Helps to describe the files (logs) data schema for DataBases (for example ClickHouse).

The file example [datamodel.conf](oap-logstream-data/src/test/resources/oap/logstream/data/map/MapLogModelTest/datamodel.conf)

* Table example:

        {
        id = REQUESTS
        type = LOG
        values = [
          {
            extends {
              path = /log
            }
          }
          {
            extends {
              path = /time
            }
          }
          {
            id = REGION
            type = STRING
            lowCardinality = true
            default = ""
            path = region
            tags = [REQUESTS, CAMPAIGN, BIDS, IMPRESSIONS, LOG]
          }
          {
            id = LOG_VOLUME
            name = Log volume
            type = DOUBLE
            default = 0.0
            path = logVolume
            tags = [REQUESTS, LOG]
          }
          {
            id = REQUEST_ID
            type = STRING
            default = ""
            path = request.id
            tags = [LOG, BIDS, REPORT]
          }
          {
            id = EXCHANGE
            name = EXCHANGE
            type = STRING
            lowCardinality = true
            default = ""
            tags = [REQUESTS, CAMPAIGN, BIDS, IMPRESSIONS, CUSTOMEVENTS, LOG]
            path = request.exchange
          }
        ]
     }
  * id - table name
  * type - table type. Can be:
    * LOG - table that is built from a file  
    * aggregate - table that is built from some aggregation operation. For example, it can be a result of joining two tables
    * empty type - usually abstract entity which can be used for several tables as `extend option`
  * values - table columns
    * extends { path = ...} - tables can extend each others. Or one table can have a few extensions 

<p>

* Column description usually looks like:

        {
          id = SSP
          type = STRING
          length = 23
          lowCardinality = true
          default = ""
          tags = [LOG, IMPRESSIONS, MMPEVENTS, INSTALLS, CUSTOMEVENTS, SRN_INSTALLS, CAMPAIGN, REPORT]
          path = impression.ssp
        }

    * id - is s column name
    * type - column schema Data Type. 
      * Can be: STRING, INTEGER, LONG, DOUBLE, BOOLEAN, ENUM, DICTIONARY, ARRAY
    * length - strict string length
    * lowCardinality - field for clickhouse optimization. Changes the internal representation of other data types to be dictionary-encoded
    * default - the default value, in case it is null
    * tags - used to mark where this filed should be shown
    * path - the object path. 
      * Can be object field or object method. For example: `response.getIPv4()` and `response.ipV4`
      * Can use concatenation. For example: path = `"response.{width,\"x\",height}"`
    
* Dictionary specific column
  
        {
          id = DEVICE_OS_VERSION
          type = ENUM
          name = Device OS Version
          dictionary = "device-os-type/**"
          default = ""
          path = request.device.osVersion
          tags = [TRAFFIC, REQUESTS, CAMPAIGN, BIDS, IMPRESSIONS, CUSTOMEVENTS, CLICKS, INSTALLS, LOG, REPORT]
        }
  * dictionary - field which represent rtb specific data type
<p>
    
* How to use datalog:
  * for logging data see: [DynamicMapLoggerTest](oap-logstream-data/src/test/java/oap/logstream/data/dynamic/DynamicMapLoggerTest.java) and 
  [MapLoggerTest](oap-logstream-data/src/test/java/oap/logstream/data/map/MapLoggerTest.java)
  * to render already logged data see: [MapLogModelTest](oap-logstream-data/src/test/java/oap/logstream/data/map/MapLogModelTest.java) 