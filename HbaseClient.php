<?php
 /**
  * Copyright (C) 2015, Lesorb, Inc.
  *
  * version 1.2
  *
  * via thrift API
  * access Hbase, get,put,scan,increment, etc.
  *
  * @category  thrift.hbase
  * @package   hbase
  * @author    lesorb <lesorb@gmail.com>
  * @copyright 2015 Lesorb, Inc.
  */
$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__);

require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Log/TLog.php' );

require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Thrift.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Transport/TBufferedTransport.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Transport/TSocket.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Transport/TFramedTransport.php' );

require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Protocol/TBinaryProtocol.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Protocol/TBinaryProtocolAccelerated.php' );

require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Type/TType.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Type/TMessageType.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Factory/TStringFuncFactory.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/StringFunc/TStringFunc.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/StringFunc/Core.php' );

require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Base/TBase.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Exception/TException.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Exception/TTransportException.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Exception/TProtocolException.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/Thrift/Exception/TApplicationException.php' );

require_once( $GLOBALS['THRIFT_ROOT'] . '/gen-php/Hbase/Types.php' );
require_once( $GLOBALS['THRIFT_ROOT'] . '/gen-php/Hbase/THBaseService.php' );

use Thrift\Transport\TSocket;
use Hbase\THBaseServiceClient;

class HbaseClient {

    public $hbase_client;
    public $transport;
    private $hbase_config;

	/**
     * configuration items of the thrift server .
     *
     * @access public
     * @param  array
	 *							demo:			array(array('db_host' => "192.168.35.143",
     *                                                      'db_user' => "",
     *                                                      'db_pass' => "",
     *                                                      'db_name' => "",
     *                                                      'db_port' => "9090",
     *                                                      ),
     *                                                array('db_host' => "192.168.35.141",
     *                                                      'db_udser' => "",
     *                                                      'db_pass' => "",
     *                                                      'db_name' => "",
     *                                                      'db_port' => "9090",
     *                                                      )
     */
    public function __construct( $config = null ) {
        if ( $config !== null )
            $this->hbase_config  =  $config;
    }

	/**
     * connect to the thrift server by configuration .
     * absolutely, once the failed, will loop request until it succeed.
     *
     * @access public
     */
    public function connect() {
        foreach($this->hbase_config as $key => $config) {
            try{
                $socket             = new TSocket($config['db_host'], $config['db_port'], $config['persist']);
                $this->transport    = new Thrift\Transport\TBufferedTransport($socket, 2048, 2048);
                $protocol           = new Thrift\Protocol\TBinaryProtocolAccelerated($this->transport);
                $this->hbase_client = new THBaseServiceClient($protocol);
                $this->transport->open();

            } catch (Exception $e) {
                //common_log( LOG_ERR, __CLASS__ . '->' . __FUNCTION__."connect hbase using host [".$config['db_host']."], port [".$config['db_port']."] exception, error msg is ->".var_export($e->getMessage(), true) );
                continue;
            }
        }
    }

    /**
     * package the put(obj) to generate columns for hbase table.
     *
     * @access public
     * @param  string	key
	 * @param  string	columnVals
	 * @return Hbase\TPut
     */
    public function generatePut($key, $columnVals) {
        return new \Hbase\TPut(array(
                'row' => $key,
                'columnValues' => $columnVals,
            ));
    }

    /**
     * get column value(obj) by family,qualifier,value and timestamp
     *
     * @access public
     * @param  string family
	 * @param  string qualifier
	 * @param  string value
	 * @param  string timestamp default 0
	 * @return  Hbase\TColumnValue
     */
    public function getColumnValue( $cf, $cq, $value, $ts=0 ) {
        $_tspec = array(
                    'family' => $cf,
                    'qualifier' => $cq,
                    'value' => $value
                );
        if( $ts !== 0 )
            $_tspes['timestamp'] = $ts;

        return new \Hbase\TColumnValue( $_tspec );
    }

	/**
     * for increated column by hbase ...
     *
     * @access public
     * @param	string	for increment table name
	 * @param	string	row name
	 * @return integer
     */
    public function increatedColumn($table, $row){
       $inc = new \Hbase\TIncrement(array(
           'row'=>$row,
           'columns'=>array(
               new \Hbase\TColumnIncrement(array(
                      'family'=>'info',
                      'qualifier'=>'incr',
                      'value'=>1
               ))
           )
       ));
       return $this->hbase_client->increment($table, $inc);
    }

	/**
     * close the thrift server connect
     *
     * @access public
     */
    public function close() {
        $this->transport->close();
    }

    function create_timestamp() {
        list($time) = explode(".", microtime(true)*1000);
        return $time;
    }
}
