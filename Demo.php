<?php
/**
 * @Author: Lesorb
 * @Date:   2017-05-23 17:59:18
 * @Last Modified by:   Lesorb
 * @Last Modified time: 2017-05-23 19:17:11
 */

defined('APP_PATH') or define('APP_PATH',dirname(__FILE__));

require_once(APP_PATH . '/DB/Class/DBHbaseThriftClass.php');
require_once(APP_PATH . '/Model/Notice_Queue_Hbase.php');

class Notice_Queue {
    public $id;
    public $data;
}

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

    // to do update&delete same as save


function common_config($main, $sub=null) {
    $_config = require_once( APP_PATH . '/conf/config.php' );
    if(isset($_config[$main])){
        if (isset($_config[$main][$sub])) {
            return $_config[$main][$sub];
        }
        return $_config[$main];
    }
    return null;
}
