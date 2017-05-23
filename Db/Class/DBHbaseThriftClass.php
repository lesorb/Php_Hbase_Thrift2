<?php
/*
 * Hbase access abstract
 * Copyright (C) 2014, 2015, Lesorb, Inc.
 *
 * The main function of this program is : abstract access API for thrift, and package the main function
 *		get,getMultiple,put,delete etc, and handler that formating there return value, and handler that
 *		do with there exception
 *
 *
 * @author lesorb <lesorb@gmail.com>
 * @date 2015/11/12
 */

require_once(APP_PATH . '/DB/Interface/DBHbaseThriftInterface.php');
abstract Class DBHbaseThriftClass implements DBHbaseThriftInterface {

    protected $_attributes = array();

    /**
     * db connector
     */
    private static $db = null;

    private $_pk;

    private $_new = true;        // whether this instance is new or not

	private $_reversal = false;

    private $__error = array();

    public function getAttributes() {
        return $this->_attributes;
    }

    /**
     * Returns if the current record is new.
     * @return boolean Defaults to false
     */
    public function getIsNewRecord() {
        return $this->_new;
    }

    /**
     * Sets if the record is new.
     * @param boolean
     */
    public function setIsNewRecord($value) {
        $this->_new=$value;
    }

    protected function _tableName() {
        return '';
    }

	public function setReversal($reversal = true) {
		$this->_reversal = $reversal;
    }

    public function setPrimaryKey($pk, $reversal=true) {
		if($reversal) {
			$this->_reversal = $reversal;
		}
		$this->_pk = $pk;
    }

	private function _reversalStr($key) {
		if($this->_reversal) {
			settype($key, 'string');
			return strrev($key);
            //return 1/log(intval($key));
		}else
			return $key;
	}

    public function getPrimaryKey() {
        if( !$this->_pk )
            $this->_pk = $this->_gerenateRowkey();
        return $this->_pk;
    }

    /**
     * single model
     */
    private static $__models = array();

    public static function instance($className=__CLASS__) {
        if(isset(self::$__models[$className]))
            return self::$__models[$className];
        else
            return self::$__models[$className]=new $className();
    }

    /**
     * get connect of thrift handle
     *
     * @access public
     * @param none
     * @return HbaseClient
     */
    public function getDb() {
        if (self::$db === null) {
            require_once(APP_PATH . '/HbaseClient.php');
            self::$db = new HbaseClient(common_config('thrift','hbase'));
        }
        return self::$db;
    }

    protected function validate($attribute) {
        return true;
    }

	/**
     *
     * created or update the key and value by a rowkey.
	 *
	 * @param array $attributes not null
	 * @param boolean $connect = 0
     * @return array
     */
    public function save($attributes=array(), $connect = true) {
        if($this->validate($attributes))
            return $this->getIsNewRecord() ? $this->insert($attributes,$connect) : $this->update($attributes,$connect);
        else
            return false;
    }

	/**
     *
     * create a new record by the rowkey and it's value,
	 *
	 * @param array $attributes not null
	 * @param boolean $connect = 1
     * @return array
     */
    public function insert($attributes,$connect = true) {

        if($this->beforeInsert()) {

            try {

                $this->getDb();

                if($connect)
                    self::$db->connect();

                //to do incr get this rowkey ...
                $ID = $this->getPrimaryKey();

                $nowTime = date('Y-m-d H:i:s');

                $attributes = array_merge($attributes, array('created_time' => $nowTime, 'modified_time' => $nowTime));

                foreach ($attributes as $key => $attribute) {
					if(null === $attribute) continue;
                    $attribute = is_array($attribute) ? json_encode($attribute) : $attribute;
                    $columnValues[] = self::$db->getColumnValue('cf',$key,$attribute);
				}

                $putInfo = self::$db->generatePut($this->_reversalStr($ID), $columnValues);

                self::$db->hbase_client->put($this->_tableName(), $putInfo);

                $this->_attributes = $attributes;

                $this->afterInsert();

                if($connect)
                    self::$db->close();

                $res = true;

            } catch(Exception $e) {

                $this->addMessage(_('hbase connect failed') .$e->getMessage());
                //common_log(LOG_ERR, __CLASS__ . '->' . __FUNCTION__.'->'._('hbase connect failed').'->'.$e->getMessage() );

                $res = false;

                if($connect)
                    self::$db->close();
            }

            return $res;
        }

        return false;
    }

	/**
     *
     * update value by the rowkey (equally put a new version)
	 *
	 * @param array $attributes not null
	 * @param boolean $connect = 0
     * @return array
     */
    public function update($attributes, $connect = true) {
        if($this->getIsNewRecord())
            throw new Exception(_('The active record cannot be updated because it is new.'));
        if($this->beforeUpdate()) {

            $this->getDb();

            if($connect)
                self::$db->connect();

            $attributes['modified_time'] = date('Y-m-d H:i:s');
            foreach ($attributes as $key => $attribute){
                if(null === $attribute) continue;
                $attribute = is_array($attribute) ? json_encode($attribute) : $attribute;
				$columnValues[] = self::$db->getColumnValue('cf', $key, $attribute);
			}

            $putInfo = self::$db->generatePut($this->_reversalStr($this->id), $columnValues);
            self::$db->hbase_client->put($this->_tableName(), $putInfo);

            $this->_attributes = $attributes;

            $this->afterUpdate();

            if($connect)
                self::$db->close();

            return true;
        }
        else
            return false;
    }

    protected function beforeInsert() {
        return true;
    }

    protected function afterInsert() {
    }

    protected function beforeUpdate() {
        return true;
    }

    protected function afterUpdate() {
    }

    protected function beforeDelete() {
        $this->delCache( $this->id );
        return true;
    }

    protected function afterDelete() {
    }

    protected function afterFindAll(&$lists) {
    }

    protected function afterFindByPk(&$attributes) {
    }

	protected function afterFindAllByStartPk(&$lists) {
	}

	/**
     *
     * delete key and value by rowkey (logical)
	 *
	 * @param boolean $connect = 0
     * @return array
     */
    public function delete($connect = true) {

        if($this->beforeDelete()) {

            $result = false;
            try {

                $this->getDb();

                if($connect)
                    self::$db->connect();

                $tDelete = new \Hbase\TDelete(array('row' => $this->_reversalStr($this->id)));
                self::$db->hbase_client->deleteSingle($this->_tableName(), $tDelete);

                $result = true;

                $this->afterDelete();

                if($connect)
                    self::$db->close();

            } catch(Exception $e) {

                $this->addMessage(_('hbase connect failed'));
                //common_log(LOG_ERR, __CLASS__ . '->' . __FUNCTION__ . ':'._('error message') .'->'. $e->getMessage().'[id is '.$this->id.']' );

                if($connect)
                    self::$db->close();
            }

            return $result;

        } else
            return false;
    }

    /**
     *
     * get related result by the rowkey(cache redis)
     *
     * @param string rowkey
     * @param boolean connect = 0
     * @param boolean cacheInstant=1
     * @param boolean noset not set attribute
     * @return array
     */
    public function findByPk($rowkey = '', $connect = true, $cacheInstant = false, $noset = true) {

        $_attr = false;
        if(!empty($rowkey)) {

            if ($cacheInstant)
                $this->delCache($rowkey);

			$CACHE_SWITCH = common_config('cache', 'enabled');
            if (false === ($CACHE_SWITCH && $_attr = Cache::instance()->get($this->_tableName().$rowkey))) {

                if ($connect)
                    $this->getDb()->connect();

                $get = new \Hbase\TGet(array('row' => $this->_reversalStr($rowkey)));

                $resultObj = $this->getDb()->hbase_client->get($this->_tableName(), $get);
                $_attr = $this->__getAttributes($resultObj);

                $this->afterFindByPk($_attr);

                if ($connect)
                    $this->getDb()->close();

                if ($CACHE_SWITCH && $_attr) {
                    Cache::instance()->set($this->_tableName().$rowkey, $_attr, null, common_config('cache', 'expire'));
                }
            }
            if ($noset)
                $this->_attributes = $_attr;
        }

        return $_attr;
    }

    /**
     *
     * According to ID and user ID to judge whether there is existing
     *
     * @param string
     * @param integer userID default 0
     * @return boolean
     */
    public function isBelongTo($ID, $userID) {
        $attribute = $this->findByPk($ID, false);
        if($userID == $attribute['uid']) {
            return true;
        }
        return false;
    }

    /**
     *
     * get all related result by the rowkeys
	 *
     * @param array $rowkeys = 0
	 * @param boolean $connect = 0
     * @return array
     */
    public function findAll(array $rowkeys, $connect = true) {

        $lists = array();

        if($connect)
            $this->getDb()->connect();

        if (common_config('cache', 'enabled')) {
            foreach ($rowkeys as $rowkey)
                $lists[] = $this->findByPk($rowkey, false, false, false);
        } else {
            foreach ($rowkeys as $rowkey)
                $gets[] = new \Hbase\TGet(array('row' => $this->_reversalStr($rowkey)));
            $_resultObjs = $this->getDb()->hbase_client->getMultiple($this->_tableName(), $gets);
            foreach ($_resultObjs as $_resultObj)
                $lists[] = $this->__getAttributes($_resultObj);
            $this->afterFindAll($lists);
        }

        if($connect)
            $this->getDb()->close();

        return $lists;
    }

	/**
     *
     * scan all results by the conditions:
	 *
     * @param array $options = 0
	 * @param boolean $connect = 0
     * @return array
     */
    public function findAllByStartPk(array $options, $connect = true) {
        $lists = array();

		if($connect)
            $this->getDb()->connect();

		$scans = new \Hbase\TScan(array('startRow' => $options['startRow'],'stopRow' => $options['stopRow']));
		$_resultObjs = $this->getDb()->hbase_client->getScannerResults($this->_tableName(), $scans, $options['limit']);
		foreach ($_resultObjs as $_resultObj)
			$lists[] = $this->__getAttributes($_resultObj);

		if(isset($options['full']) && $options['full'])
			$this->afterFindAllByStartPk($lists);

		if($connect)
            $this->getDb()->close();

		return $lists;
	}

    /**
     *
     * convert Hbase\TResult to array from thrift API.
     *
     * @param Hbase\TResult result
     * @return array
     */
    private function __getAttributes(Hbase\TResult $result) {

        if (null === $result->row) {
            return array();
        }

        $attribute = $this->getAttributes();
        $attribute['id'] = $this->_reversalStr($result->row);

        foreach ($result->columnValues as $cK => $columnVal) {
            if (in_array( $columnVal->qualifier, array('editor_ids','group_ids','attachments','publish_scope')) && !empty($columnVal->value)) {
                $columnVal->value = json_decode(trim($columnVal->value), true);
            }
            if (in_array($columnVal->qualifier, array('modified_time','created_time')))
                $columnVal->value = strtotime($columnVal->value);
            $attribute[$columnVal->qualifier] = $columnVal->value;
        }

        return $attribute;
    }

    /**
     *
     * delete related cache key via the rowkey(redis)
     *
     * @param string rowkey
     * @return none
     */
    public function delCache($rowkey) {
        if (common_config('cache', 'enabled'))
            Cache::instance()->delete($this->_tableName().$rowkey);
    }

    protected function validateID($id) {
        return true;
    }

    public function __set($name,$value) {
        if(property_exists($this,$name))
            return $this->$name;
        elseif(isset($this->_attributes[$name]))
            $this->_attributes[$name]=$value;
    }

    public function __get($name) {
        if(property_exists($this,$name))
            return $this->$name;
        if(isset($this->_attributes[$name]))
            return $this->_attributes[$name];
    }

    protected function _gerenateRowkey() {
        return $this->_autoIncrRowkey($this->_tableName());
    }

    /**
     *
     * get auto-incrementing rowkey via table and rowkey(initial key)
     *
     * @param string $table = ''
     * @param boolean $direct default value false
     * @return mixed
     */
    protected function _autoIncrRowkey($table, $direct=false) {
        $incrKey = $this->__incrRedis($table);
        $incrKey .= $direct ? '' : $this->__incrTime();
        return $incrKey;
    }

    private function __incrTime($prefix = '') {
        list($date,$time) = explode('.', microtime(true));
        $rowkeyIncr = $prefix . (float)$date . (float)$time;
        return $rowkeyIncr;
    }

    // public function unsetInstance($className=__CLASS__) {
    //     if(isset(self::$__models[$className]))
    //         unset(self::$__models[$className]);
    // }

    public function __destruct() {
        $this->_attributes = array();
        $this->_pk = null;
        $this->_new = true;
        $this->_reversal = false;
        $this->__error = array();
        // $this->unsetInstance();
    }

    /**
     *
     * auto-incrementing rowkey(Redis)
     *
     * @param string $table = ''
     * @return string
     */
    private function __incrRedis($table) {
        return Cache::instance()->increment($table);
    }

    /**
     * set error message for debug or log record.
     * @param integer $code
     * @return bool
     */
    public function addMessage($error) {
        $this->__error[] = $error;
    }

    public function getMessage($string = true) {
        if($string)
            return join(' ', $this->__error);
        return $this->__error;
    }

}
