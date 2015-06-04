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