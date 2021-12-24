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
 
[DataModel.java](oap-logstream-data/src/main/java/oap/logstream/data/DataModel.java) helps to describe the files (logs) 
data schema for DataBases (for example ClickHouse).

The file example [datamodel.conf](oap-logstream-data/src/test/resources/oap/logstream/data/map/MapLogModelTest/datamodel.conf)

* Column description usually looks like:

        {
          id = SSP
          type = STRING
          length = 23
          default = ""
          tags = [LOG, IMPRESSIONS, MMPEVENTS, INSTALLS, CUSTOMEVENTS, SRN_INSTALLS, CAMPAIGN, REPORT]
          path = impression.ssp
        }

    * id - is s column name
    * type - column schema Data Type
    * length - strict string length 
    * default - the default value, in case it is null
    * tags - used to mark where this filed should be shown
    * path - the object path
<p>
    
* How to use datalog:
  * for logging data see: [DynamicMapLoggerTest](oap-logstream-data/src/test/java/oap/logstream/data/dynamic/DynamicMapLoggerTest.java) and 
  [MapLoggerTest](oap-logstream-data/src/test/java/oap/logstream/data/map/MapLoggerTest.java)
  * to render already logged data see: [MapLogModelTest](oap-logstream-data/src/test/java/oap/logstream/data/map/MapLogModelTest.java) 