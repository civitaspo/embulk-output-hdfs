# Hdfs output plugin for Embulk

A File Output Plugin for Embulk to write HDFS.

## Overview

* **Plugin type**: file output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **config_files** list of paths to Hadoop's configuration files (array of strings, default: `[]`)
- **config** overwrites configuration parameters (hash, default: `{}`)
- **output_path** the path finally stored files. (string, default: `"/tmp/embulk.output.hdfs_output.%Y%m%d_%s"`)
- **working_path** the path temporary stored files. (string, default: `"/tmp/embulk.working.hdfs_output.%Y%m%d_%s"`)

## Example

```yaml
out:
  type: hdfs
  config_files:
    - /etc/hadoop/conf/core-site.xml
    - /etc/hadoop/conf/hdfs-site.xml
    - /etc/hadoop/conf/mapred-site.xml
    - /etc/hadoop/conf/yarn-site.xml
  config:
    fs.defaultFS: 'hdfs://hdp-nn1:8020'
    dfs.replication: 1
    mapreduce.client.submit.file.replication: 1
    fs.hdfs.impl: 'org.apache.hadoop.hdfs.DistributedFileSystem'
    fs.file.impl: 'org.apache.hadoop.fs.LocalFileSystem'
  formatter:
    type: csv
    encoding: UTF-8
```


## Build

```
$ ./gradlew gem
```

## Development

```
$ ./gradlew classpath
$ bundle exec embulk run -I lib example.yml
```
