OAP-logstream Binary protocol encoding V2
===============================

This document describes the wire encoding for OAP-logstream messages.

# Binary protocol

```
+---------------------+-------------+
| oap.logstream.LogId | Binary data |
+---------------------+-------------+
```


## oap.logstream.LogId

```
+------------ +-------------------+---------+----------------+-------+--------------+---------+-------+-----------------+------------+
| digestionId | filePrefixPattern | logType | clientHostname | shard | headers size | headers | types | properties size | properties |
+-------------+-------------------+---------+----------------+-------+--------------+---------+-------+-----------------+------------+
```

Where:
* `digestionId` is a 8-byte digestion id ( `Cuid.UNIQUE#nextLong()` )
* `filePrefixPattern` UTF-8-encoded string 
* `logType` UTF-8-encoded string
* `clientHostname` UTF-8-encoded string
* `shard` is a 4-byte int
* `headers size` is the size of the headers list, encoded as a single-byte, positive values only
* `headers` UTF-8-encoded strings.
* `types` a single-byte list of type lists
* `properties size` is the size of the properties list, encoded as a single-byte, positive values only
* `properties` N key-value pairs encoded as UTF-8, where N is the number of properties. 

## types
0. EOL
1. RAW
2. DATETIME
3. DATE
4. BOOLEAN
5. BYTE
6. SHORT for example [6]
7. INTEGER
8. LONG
9. FLOAT
10. DOUBLE
11. STRING
12. LIST - complex type, subtype must be specified, for example LIST(BYTE) - [12,5]