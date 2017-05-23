<?php
/*
 * Hbase access interface
 * Copyright (C) 2014, 2015, Lesorb, Inc.
 *
 * The main function of this program is : interface of access thrift definition.
 *
 *
 * @author lesorb <lesorb@gmail.com>
 * @date 2015/11/12
 */
interface DBHbaseThriftInterface {

    public function delete( $connect );

    public function insert( $attributes,$connect );

    public function update( $attributes,$connect );

    public function findByPk( $rowkey,$connect,$cacheInstant );

    public function findAll( array $rowkeys,$connect );

}
