# Hdfs file output plugin for Embulk

[![Build Status](https://travis-ci.org/civitaspo/embulk-output-hdfs.svg)](https://travis-ci.org/civitaspo/embulk-output-hdfs)
[![Coverage Status](https://coveralls.io/repos/github/civitaspo/embulk-output-hdfs/badge.svg?branch=master)](https://coveralls.io/github/civitaspo/embulk-output-hdfs?branch=master)

A File Output Plugin for Embulk to write HDFS.

## Overview

* **Plugin type**: file output
* **Load all or nothing**: yes
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **config_files**: list of paths to Hadoop's configuration files (array of strings, default: `[]`)
- **config**: overwrites configuration parameters (hash, default: `{}`)
- **path_prefix**: prefix of target files (string, required)
- **file_ext**: suffix of target files (string, required)
- **sequence_format**: format for sequence part of target files (string, default: `'%03d.%02d.'`)
- **rewind_seconds**: When you use Date format in path_prefix property(like `/tmp/embulk/%Y-%m-%d/out`), the format is interpreted by using the time which is Now minus this property. (int, default: `0`)
- **doas**: username which access to Hdfs (string, default: executed user)
- **overwrite** *(Deprecated: Please use `mode` option instead)*: overwrite files when the same filenames already exists (boolean, default: `false`)
    - *caution*: even if this property is `true`, this does not mean ensuring the idempotence. if you want to ensure the idempotence, you need the procedures to remove output files after or before running. 
- **delete_in_advance** *(Deprecated: Please use `mode` option instead)*: delete files and directories having `path_prefix` in advance (enum, default: `NONE`)
    - `NONE`: do nothing
    - `FILE_ONLY`: delete files
    - `RECURSIVE`: delete files and directories
- **mode**: "abort_if_exist", "overwrite", "delete_files_in_advance", "delete_recursive_in_advance", or "replace". See below. (string, optional, default: `"abort_if_exist"`)
    * In the future, default mode will become `"replace"`.

## CAUTION
If you use `hadoop` user (hdfs admin user) as `doas`, and if `delete_in_advance` is `RECURSIVE`,
`embulk-output-hdfs` can delete any files and directories you indicate as `path_prefix`,
this means `embulk-output-hdfs` can destroy your hdfs.
So, please be careful when you use `delete_in_advance` option and `doas` option ...

## About DELETE

When this plugin deletes files or directories, use [`Hadoop Trash API`](https://hadoop.apache.org/docs/r2.8.0/api/org/apache/hadoop/fs/Trash.html). So, you can find them in the trash during `fs.trash.interval`.

## Modes

* **abort_if_exist**:
    * Behavior: This mode writes rows to the target files in order. If target files already exist, abort the transaction.
    * Transactional: No. If fails, the target files could have some rows written.
    * Resumable: No.
* **overwrite**:
    * Behavior: This mode writes rows to the target files in order. If target files already exist, this re-write from the beginning of the file.
    * Transactional: No. If fails, the target files could have some rows written.
    * Resumable: No.
* **delete_files_in_advance**:
    * Behavior: This mode delete files at first, then writes rows to the target files in order.
    * Transactional: No. If fails, the target files could be removed.
    * Resumable: No.
* **delete_recursive_in_advance**:
    * Behavior: This mode delete directories recursively at first, then writes rows to the target files in order.
    * Transactional: No. If fails, the target files could be removed.
    * Resumable: No.
* **replace**:
    * Behavior: This mode writes rows to the workspace files in order, then replace them to target directories. This **replace** is not **atomic** because hdfs api does not have atomic replace. 
    * Transactional: No. If fails, the target files could be removed. 
    * Resumable: No.

## Example

```yaml
out:
  type: hdfs
  config_files:
    - /etc/hadoop/conf/core-site.xml
    - /etc/hadoop/conf/hdfs-site.xml
  config:
    fs.defaultFS: 'hdfs://hdp-nn1:8020'
    fs.hdfs.impl: 'org.apache.hadoop.hdfs.DistributedFileSystem'
    fs.file.impl: 'org.apache.hadoop.fs.LocalFileSystem'
  path_prefix: '/tmp/embulk/hdfs_output/%Y-%m-%d/out'
  file_ext: 'txt'
  overwrite: true
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
$ embulk run -I lib example/config.yml
```
