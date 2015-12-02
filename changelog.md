ChangeLog jsHS2
======

# 0.2.0
* Support promise (using Promise ^7.0.1), or callback
* Add new idl (Thrift_0.9.2_Hive_1.1.0, Thrift_0.9.2_Hive_1.2.0)
* Add new feature, support getLog
    * if you use version that is more than HIVE\_CLI\_SERVICE\_PROTOCOL\_V6, use getLog
    * if you use CDH Hive, use getLog
* Add new feature, Configure and IdlContainer
    * merge connection & cursor configuration
    * IdlContainer is dynamic load IDL
    
    
# 0.2.1
* Change Configure -> Configuration
    * use noun
* Change error message layout
    * return error message and info message
* Change IdlContainer
    * implementation callback version
    * support promise version using by callback
    * move to Configuration, because every Configurations have each IdlContainer. Each Configurations has 
    differents IdlContainer for execute multi version executing
* Change ConnectionTest

# 0.2.2
* Change changelog.md

# 0.2.7
* Fix require error

# 0.2.8
* Change README.md
* add CallbackTest.js
* rename ConnectionTest.js -> PromiseTest.js

# 0.2.9
* Bug-fix on CConnection.js (not called thriftConnection.end())

# 0.2.10
* Add option runAsync in execute
    * This options for cli script
* Fix idlFactory module loading bug (path problem, fixed it)
* Fix cluster.json.sample
    * cluster.json.sample so hard readable, that is fix more readable and easy
    * cp cluster.json.sample cluster.json
    * Remove comment (that is starts with '//') and write your environment
* deps: co@^4.6.0
* deps: mocha@^2.3.4
* deps: thrift@^0.9.3
* deps: lodash@^3.10.1

# 0.2.11
* fix runAsync option bug
    * runAsync not working on PCursor.execute
* Add util isFinish and cSleep, pSleep
* Fix testcase. Now test sync and async api
* deps: chai@^3.4.1
* deps: debug@^2.2.0

# 0.2.12
* Fix bug from CallbackTest and PromiseTest. If resultset hasn't rows after cause error. Because test always try getSchema and fetch.
* Fix bug from CCursor. If error caused from close, return reject. But reject not defined function, fix it.
* add Configuration option i64ToString. int64 convert i64val to float64. Int64 cannot convert float64, return Infinity. i64ToString is to true, convert string value of real value. 
    * If you not set i64ToString that is to set true. You don't wanna this feature that flag set false.
    * update cluster.json
    * update testcase

```
# example

i64ToString true, 7614985126350998549 -> '7614985126350998549' 
i64ToString false, 7614985126350998549 -> Infinity
```