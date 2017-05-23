# Php_Hbase_Thrift2

Base on Apache PHP thrift .
Packaged the classes and basic methods for operating Hbase, simplifying the calling API.

# Base:

Thrift.

License		ASF
Description	Thrift is a lightweight, language-independent software stack with an associated code generation mechanism for RPC.
Homepage	https://github.com/apache/thrift

# Hierarchy

conf/

Db/
    Class/
    Interface/    
    
    Packaged abstract superclass and interface

gen-php/

Model/

thrift/
    Empty folder, You can generate thrift2 of php library by the Apache thrift.

HbaseClient.php
Demo.php

-------------------------------------------------
## Usage

Show code as Demo.php here:
~~~~
    $Notice_Queue = new Notice_Queue();
    //id is row key that will automatically gerenate if is not set here.
    $Notice_Queue->id = '12345';
    $Notice_Queue->data = '{"parent":"1034","created_time":"1453974270106","modified_time":"1453974270106","verb":"delete","user_id":61506804,"site_id":72399,"source":"api","object_type":"board","board_id":"1","is_comment":0,"conversation_id":"43"}';

    is_string($Notice_Queue->data) && $Notice_Queue->data = json_decode($Notice_Queue->data, true);
    Notice_Queue_Hbase::instance()->setIsNewRecord(true);
    Notice_Queue_Hbase::instance()->setPrimaryKey($Notice_Queue->id);
    Notice_Queue_Hbase::instance()->id = $Notice_Queue->id;

    if(Notice_Queue_Hbase::instance()->save($Notice_Queue->data) === false) {
        //Exception
    } else {
       echo 'save successful';
    }
~~~~
   cli : php Demo.php

# Notice

    Before you run the Demo.php, you must create two tables in the Hbase based on annotations in code files in Db.

-------------------------------------------------
## Contact me
lesorb@gmail.com

