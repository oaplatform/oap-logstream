OAP-logstream Binary protocol encoding V1
===============================

This document describes the wire encoding for OAP-logstream messages.

# Binary protocol

```
+---------------------+-------------+
| oap.logstream.LogId | String data |
+---------------------+-------------+
```
Where:
* `String data` UTF-8-encoded string


## oap.logstream.LogId

```
+------------ +-------------------+---------+----------------+-------+---------+-----------------+------------+
| digestionId | filePrefixPattern | logType | clientHostname | shard | headers | properties size | properties |
+-------------+-------------------+---------+----------------+-------+---------+-----------------+------------+
```

Where:
* `digestionId` is a 8-byte digestion id ( `Cuid.UNIQUE#nextLong()` )
* `filePrefixPattern` UTF-8-encoded string 
* `logType` UTF-8-encoded string
* `clientHostname` UTF-8-encoded string
* `shard` is a 4-byte int
* `headers` UTF-8-encoded string. A tab-separated list of column names.
* `properties size` is the size of the properties list, encoded as a single-byte, positive values only
* `properties` N key-value pairs encoded as UTF-8, where N is the number of properties. 
