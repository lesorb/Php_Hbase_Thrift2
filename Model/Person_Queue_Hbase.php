<?php
/**
 * Person queue_Hbase - deal with data for access hbase
 *
 * sql: create 'person_queue',{NAME=>'cf'}
 *
 * @category Notices
 * @package  classes
 * @author   lesorb <lesorb@gmail.com>
 * Copyright (C) 2014-2015 Lesorb, Inc.
 */

class Person_Queue_Hbase extends DBHbaseThriftClass {

	const PERSON = 'p';

	const PERSON_EDIT = 'e';

    //definition of object type here.
    public static $TYPE = array(
                        'board' => 'bo',
                    );

	public static function instance($className=__CLASS__) {
        return parent::instance($className);
    }

	/**
	 * Table Definition for notice
	 */
	public function _tableName() {
        return 'person_queue';
    }

	/**
     *
     * Set a value by pk(any string/integer) & type(object type).
     * Unlike other decomposition solutions, due to this algorithm routine, there are promise
     * that (person|notice) queue save in Hbase in reverse order by the pk, does not attenuation
     * fast when pk increase a Billions integer number.
     *
     * @param string pk
     * @param string type
     * @return string
     * @access public
     */
    public function setPKeyByType($pk, $type='') {
        $pk = 1 == $pk ? 1 :($pk > 1 ? 1/log($pk) : '');
        return self::$TYPE[$type].$pk;
    }

    public function setPrimaryKey($pk, $reversal=false) {
        return parent::setPrimaryKey($pk, $reversal);
    }

	protected $_attributes = array(
                    'id' => '',
                    'type' => '',//board,bbs,status,qa: verb in[retweet create]
					'user_id' => '0',
                    'notice_id' => '0',
                    'created_at' => '',
					'created_time' => '',
                    'modified_time' => '',
                );

	/**
      *
      * To find list result of person queue by the options, that include:
      * bbs,board,status,qa, a definition in this TYPE.
      *
      *DEMO:
	  *arary(
	  *	'user_id'=>'',		//needed
	  *	'notice_id'=>,		//needed
	  *	'object_type'=>,	//needed
	  *	'person'=>	//boolean
	  *	'full'=>	//boolean
	  *	)
      *
      * @param array options
      * @param boolean connect
      * @return array
	  */
	public function findAllByStartPk(array $options, $connect = true) {
		$_startRow = $this->setPKeyByType($options['notice_id'], $options['object_type']);
		$_stopRow = $this->setPKeyByType(0, $options['object_type']);

        if(isset($options['person']) && true === $options['person']) {
			$_startRow = self::PERSON.':'.$_startRow;
            $_stopRow = self::PERSON.':'.$_stopRow;
        }
		if($options['notice_id'] < 1)
			$_startRow .= '0./';
		$_startRow = $options['user_id'].$_startRow.'/';
        $_stopRow = $options['user_id'].$_stopRow.'0.:';
		$conditions = array(
			'startRow' => $_startRow,
            'stopRow' => $_stopRow,
			'limit' => isset($options['limit']) ? $options['limit'] : 10,
			'full' => isset($options['full']) ? $options['full'] : false,
		);
		return parent::findAllByStartPk($conditions, $connect);
	}

    /**
     *
     * Hook function, call depends on function findAllByStartPk
     *
     * @param array &lists
     * @return none
     * @access protected
     */
	protected function afterFindAllByStartPk(&$lists) {
        if(!empty($lists)) {
			foreach($lists as $list) {
				$_ids[] = $list['notice_id'];
			}
            Notice_Queue_Hbase::instance()->setReversal();
			$lists = Notice_Queue_Hbase::instance()->findAll($_ids, false);
        }
	}
}
