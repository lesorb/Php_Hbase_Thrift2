<?php
/**
 * @Author: Wanglj
 * @Date:   2017-05-23 18:37:15
 * @Last Modified by:   Wanglj
 * @Last Modified time: 2017-05-23 18:46:03
 */
return array(
        'cache' => array(
                'enabled' => true,
                'expire' => 86400,
            ),
        'thrift' => array(
            'incr' => array(
                    '1' => 'Hbase',
                    '2' => 'Redis',
                    '3' => 'Time',
                ),
            'incr_type' => 3,
            'hbase' => array(
                   array(
                        'db_host' => '192.168.0.23',
                        'db_user' => '',
                        'db_pass' => '',
                        'db_name' => '',
                        'db_port' => '9090',
                        'persist' => false
                    ),
                   array(
                        'db_host' => '192.168.0.22',
                        'db_user' => '',
                        'db_pass' => '',
                        'db_name' => '',
                        'db_port' => '9090',
                        'persist' => false
                    )
                )
            ),

    );