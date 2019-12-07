0.4.0.pre (2019-12-07)
======================
* Use Github Actions instead of TravisCI
* Update dependencies (java 7 -> 8, embulk v0.8.38 -> v0.9.20, hadoop-client 2.6.0 -> 2.9.2)
* Add `UserGroupInformation#setConfiguration` for kerberos authentication

0.3.0 (2017-12-03)
==================
* Add: `mode` option.
    * Add `replace` behaviour.
* Deprecated: `delete_in_advance` option. Please use `mode` instead.
* Deprecated: `overwrite` option. Please use `mode` instead.
* Enhancement: Delete behaviour become safely.
* Enhancement: Update embulk 0.8.38

0.2.4 (2016-04-27)
==================
- Enhancement: Avoid to create 0 byte files
  - https://github.com/civitaspo/embulk-output-hdfs/pull/14

0.2.3 (2016-04-20)
==================
- Add: `delete_in_advance` option

0.2.2 (2016-02-02)
==================
- Add: doas option
