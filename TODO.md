JSHS2
====

# 0.3.0
* Add SASL protocol handshake
* Re-compile idl using by Thrift 0.9.3
* Remove callback interface
    * Because, latest LTS version 4.2.2, that have promise
    * Thrift 0.9.3 compile idl, compiled idl already use Promise