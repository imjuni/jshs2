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