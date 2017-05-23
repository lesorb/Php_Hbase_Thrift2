<?php
/**
 * Notice_queue_Hbase - deal with data for access hbase
 *
 * sql: create 'notice_queue',{NAME=>'cf',REPLICATION_SCOPE=>1}
 *
 * @category Notices
 * @package  classes
 * @author   lesorb <lesorb@gmail.com>
 * Copyright (C) 2014-2015 Lesorb, Inc.
 */


/* We put in some notices this table, all vailable per API request,
 * in the redis cache. */

class Notice_Queue_Hbase extends DBHbaseThriftClass {

	public static function instance($className=__CLASS__) {
        return parent::instance($className);
    }

    //definition of edit user id for bbs here.
    protected $edit_userids = array();

    //definition of group user id here.
    protected $group_userids = array();

    public function setEditUsers(array $user_ids) {
        $this->edit_userids = $user_ids;
    }

    public function setGroupUsers(array $user_ids) {
    	$this->group_userids = $user_ids;
    }

	/**
	 * Table Definition for notice
	 */
	protected function _tableName() {
        return 'notice_queue';
    }

	protected $_attributes = array(
                    'id' => '',
					'notice_id' => '0',
                    'text' => '',
					'truncated' => '',
					'created_at' => '',
					'modified_at' => '',
					'in_reply_to_status_id' => '',
					'uri' => '',
					'source' => '',
					'in_reply_to_user_id' => '',
					'in_reply_to_screen_name' => '',
					'geo' => '',
					'user_id' => '',
					'verb' => '',
					'object_type' => '',
					'board_id' => '',
					'title'	 => '',
					'attachments' => array(),
					'attitudes' => '',
					'favorited' => '',
					'repeated' => '',
					'repeated_id' => '',
					'conversation_id' => '',
					'favourites_count' => '0',
					'retweet_count' => '0',
					'attitudes_count' => '0',
					'attention_count' => '0',
					'comment_count' => '0',
					'attention_count' => '0',
					'board_name' => '',
					'group_ids' => array(),
					'editor_ids' => array(),
                    'publish_scope' => array(),
                    'created_time' => '',
                    'modified_time' => '',
                );

    /**
     *
     * Hook function, call depends on function insert.
     *
     * @param none
     * @return none
     * @access protected
     */
    protected function afterInsert() {
		if (in_array($this->verb, array('create', 'retweet'))) {
			$_id = $this->getPrimaryKey();
            $_attr = array(
		       				'type' => $this->object_type,
		    				'notice_id' => $_id,
		    				'created_at' => $this->created_at,
		    			);
			$_pk = Person_Queue_Hbase::instance()->setPKeyByType($_id, $this->object_type);
			$__pk = $this->user_id.Person_Queue_Hbase::PERSON.':'.$_pk;
			Person_Queue_Hbase::instance()->setPrimaryKey($__pk);
		    Person_Queue_Hbase::instance()->id = $__pk;
			$_attr['user_id'] = $this->user_id;
	    	Person_Queue_Hbase::instance()->save($_attr, false);

	    	if(!empty($this->group_userids)) {
				foreach ($this->group_userids as $user_id) {
					$__pk = $user_id.$_pk;
					Person_Queue_Hbase::instance()->setPrimaryKey($__pk);
			    	Person_Queue_Hbase::instance()->id = $__pk;
			    	$_attr['user_id'] = $user_id;
			    	Person_Queue_Hbase::instance()->save($_attr, false);
		    	}
				$this->group_userids = array();
    		}
            if(!empty($this->edit_userids)) {
                $__pk = Person_Queue_Hbase::PERSON_EDIT.':'.$_pk;
                foreach ($this->edit_userids as $user_id) {
                    $___pk = $user_id.$__pk;
                    Person_Queue_Hbase::instance()->setPrimaryKey($___pk);
                    Person_Queue_Hbase::instance()->id = $___pk;
                    $_attr['user_id'] = $user_id;
                    Person_Queue_Hbase::instance()->save($_attr, false);
                }
				$this->edit_userids = array();
            }
    	}
		//Notice_Queue_Related_Hbase::instance()->renewByNotice($this);
    }

	/**
     *
     * Hook function, call depends on function update.
     *
     * @param none
     * @return none
     * @access protected
     */
	protected function afterUpdate() {
		if(!$this->parent)
			return ;
		$upNotice = new Notice_Queue_Hbase();
		$upNotice->setReversal(true);
		if(false !== ($upAttr = $upNotice->findByPk($this->parent, false, true))) {
			$_id = $upNotice['id'];
			$_attr = array(
		       				'type' => $this->object_type,
		    				'notice_id' => $upNotice['id'],
		    				'created_at' => $this->created_at,
		    			);
			$_pk = Person_Queue_Hbase::instance()->setPKeyByType($_id, $this->object_type);
			//this->group_ids is to be update.
			if($this->group_ids) {
				$_diff = array_diff($this->group_ids, $upAttr['group_ids']);
				if(!empty($_diff)) {
					if(!empty($upAttr['group_ids'])) {
						foreach ($upAttr['group_ids'] as $user_id) {
							$__pk = $user_id.$_pk;
							Person_Queue_Hbase::instance()->setPrimaryKey($__pk);
							Person_Queue_Hbase::instance()->id = $__pk;
							Person_Queue_Hbase::instance()->delete(false);
						}
					}
					foreach ($this->group_userids as $user_id) {
						$__pk = $user_id.$_pk;
						Person_Queue_Hbase::instance()->setPrimaryKey($__pk);
						Person_Queue_Hbase::instance()->id = $__pk;
						$_attr['user_id'] = $user_id;
						Person_Queue_Hbase::instance()->save($_attr, false);
					}
				}
			}
			//this->editor_ids is to be update.
			if($this->editor_ids) {
				$_diff = array_diff($this->editor_ids, $upAttr['editor_ids']);
				$__pk = Person_Queue_Hbase::PERSON_EDIT.':'.$_pk;
				if(!empty($_diff)) {
					if(!empty($upAttr['editor_ids'])) {
						foreach ($upAttr['editor_ids'] as $user_id) {
							$___pk = $user_id.$__pk;
							Person_Queue_Hbase::instance()->setPrimaryKey($___pk);
							Person_Queue_Hbase::instance()->id = $___pk;
							Person_Queue_Hbase::instance()->delete(false);
						}
					}
					foreach ($this->editor_ids as $user_id) {
						$___pk = $user_id.$__pk;
						Person_Queue_Hbase::instance()->setPrimaryKey($___pk);
						Person_Queue_Hbase::instance()->id = $___pk;
						$_attr['user_id'] = $user_id;
						Person_Queue_Hbase::instance()->save($_attr, false);
					}
				}
			}
		}
		unset($upNotice);
	}
}
