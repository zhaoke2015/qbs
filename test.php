<?php
use DB\DbConnection;
require_once __DIR__.'/DbConnection.php';
$dbConnection = new DbConnection('mysql:host=127.0.0.1;dbname=zgoubao', 'root', '398062080');
$sqlCommand = $dbConnection->createCommand();
$data = $sqlCommand
	->select('c.name,c.id,p.name as pname')
	->from('product_category c')
	->leftJoin('product_category p', 'c.pid=p.id')
	->order('c.id desc')
	->limit(20)
	->where(array('c.id <' => 10))
	->queryAll();
print_r($sqlCommand->getLastSql()."\n");
print_r($data);

print_r($dbConnection->getAllColumnNames('product_category'));