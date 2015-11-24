<?php
namespace DB;
/**
 * 数据库连接对象
 * @author xiawei
 */
class DbConnection extends \PDO {
	/**
	 * 是否在事物中
	 * @var boolean
	 */
	private $isTransaction;
	
	/**
	 * 创建一个SqlCommand
	 * @param string $sql
	 * @return \DB\SqlCommand
	 */
	public function createCommand($sql = null) {
		return new SqlCommand($this, $sql);
	}
	
	/**
	 * (non-PHPdoc)
	 * @see PDO::beginTransaction()
	 */
	public function beginTransaction() {
		if ($this->isTransaction) {
			$this->throwException("Has a Transaction which is not commit or rollback");
		}
		return parent::beginTransaction();
	}
	
	/**
	 * (non-PHPdoc)
	 * @see PDO::commit()
	 */
	public function commit() {
		if ($this->isTransaction) {
			return parent::commit();
		} else {
			$this->throwException("Has no connection in Transaction");
		}
	}
	
	/**
	 * (non-PHPdoc)
	 * @see PDO::rollBack()
	 */
	public function rollBack() {
		if ($this->isTransaction) {
			return parent::rollBack();
		} else {
			$this->throwException("Has no connection in Transaction");
		}
	}
	
	
	/**
	 * 抛出异常
	 * @param string $message 异常信息
	 * @param string $code    异常代码
	 * @throws \Exception
	 */
	public function throwException($message = null, $code = null) {
		$errorInfo = $this->errorInfo();
		if (!empty($errorInfo)) {
			if (empty($message)) {
				$message = $errorInfo[2];
			}
			if (empty($code)) {
				$code = $errorInfo[0];
			}
		}
		throw new \Exception($message, $code, null);
	}
	
	/**
	 * 获取所有列信息
	 * @param unknown $tableName
	 * @return Ambigous <multitype:, multitype:mixed >
	 */
	public function getAllColumn($tableName) {
		return $this->createCommand()->queryAll("SHOW COLUMNS FROM {$tableName}");
	}
	
	/**
	 * 获取所有的列名
	 * @param unknown $tableName
	 * @return multitype:
	 */
	public function getAllColumnNames($tableName) {
		return \array_column($this->getAllColumn($tableName), 'Field');
	}
}

/**
 * Sql命令相关的对象
 * @author xiawei
 */
class SqlCommand {
	/**
	 * 对应的数据库连接
	 * @var DbConnection
	 */
	private $connection;
	
	/**
	 * 对应的
	 * @var string
	 */
	private $sql;
	
	
	private $select_str = '';
	private $select_from = '';
	private $select_where = '';
	private $select_join = '';
	private $select_group = '';
	private $select_having = '';
	private $select_order = '';
	private $select_limit = '';
	
	private $lastSql;
	
	/**
	 * 构造方法
	 * @param DbConnection $connection
	 * @param string $sql
	 */
	public function __construct(DbConnection $connection, $sql = null) {
		$this->connection = $connection;
		$this->sql = $sql;
	}
	
	private function buildSql() {
		return "{$this->select_str} {$this->select_from} {$this->select_join} {$this->select_where} {$this->select_group} {$this->select_having} {$this->select_order} {$this->select_limit}";
	}
	
	/**
	 * 获取要执行的sql
	 * @return string
	 */
	private function getSql() {
		if (empty($this->sql)) {
			$this->sql = $this->buildSql();
		}
		return $this->sql;
	}
	
	/**
	 * 运行一条sql
	 * @param string $sql
	 * @return number
	 */
	public function exec($sql = null) {
		if ($sql = null) {
			$sql = $this->getSql();
		}
		$this->lastSql = $sql;
		return $this->connection->exec($sql);
	}
	
	/**
	 * 执行一条查询命令
	 * @param string $sql
	 * @return array
	 */
	public function query($sql = null) {
		if (empty($sql)) {
			$sql = $this->getSql();
		}
		$this->lastSql = $sql;
		$statement = $this->connection->query($sql);
		if (empty($statement)) {
			$this->throwException();
		}
		return $statement;
	}
	
	public function getLastSql() {
		return $this->lastSql;
	}
	
	/**
	 * 抛出异常
	 * @param string $message 异常信息
	 * @param string $code    异常代码
	 * @throws \Exception
	 */
	private function throwException($message = null, $code = null) {
		$errorInfo = $this->connection->errorInfo();
		if (!empty($errorInfo)) {
			if (empty($message)) {
				$message = $errorInfo[2];
			}
			if (empty($code)) {
				$code = $errorInfo[0];
			}
		}
		throw new \Exception($message, $code, null);
	}
	
	private function buildWhere($condition = array(), $logic = 'AND') {
		$s = $this->buildCondition($condition, $logic);
		if ($s)
			$s = ' WHERE ' . $s;
		return $s;
	}
	
	private function buildCondition($condition = array(), $logic = 'AND') {
		if (!\is_array($condition)) {
			if (\is_string($condition)) {
				$count = \preg_match('#\>|\<|\=| #', $condition, $logic);
				if (!$count) {
					$this->throwException('bad sql condition: must be a valid sql condition');
				}
				$condition = \explode($logic[0], $condition);
				if(!\is_numeric($condition[0]))
				{
					$condition[0] = $this->quoteObj($condition[0]);
				}
				$condition = \implode($logic[0], $condition);
				return $condition;
			}
	
			$this->throwException('bad sql condition: ' . \gettype($condition));
		}
		$logic = \strtoupper($logic);
		$content = null;
		foreach ($condition as $k => $v) {
			$v_str = null;
			$v_connect = '';
	
			if (\is_int($k)) {
				if ($content)
					$content .= $logic . ' (' . $this->buildCondition($v) . ') ';
				else
					$content = '(' . $this->buildCondition($v) . ') ';
				continue;
			}
	
			$k = trim($k);
	
			$maybe_logic = \strtoupper($k);
			if (\in_array($maybe_logic, array('AND', 'OR'))) {
				if ($content)
					$content .= $logic . ' (' . $this->buildCondition($v, $maybe_logic) . ') ';
				else
					$content = '(' . $this->buildCondition($v, $maybe_logic) . ') ';
				continue;
			}
	
			$k_upper = \strtoupper($k);
			$maybe_connectors = array('>=', '<=', '<>', '!=', '>', '<', '=',
					' NOT BETWEEN', ' BETWEEN', 'NOT LIKE', ' LIKE', ' IS NOT', ' NOT IN', ' IS', ' IN');
			foreach ($maybe_connectors as $maybe_connector) {
				$l = \strlen($maybe_connector);
				if (\substr($k_upper, -$l) == $maybe_connector) {
					$k = \trim(\substr($k, 0, -$l));
					$v_connect = $maybe_connector;
					break;
				}
			}
			if (is_null($v)) {
				$v_str = ' NULL';
				if ($v_connect == '') {
					$v_connect = 'IS';
				}
			} else if (\is_array($v)) {
				if ($v_connect == ' BETWEEN') {
					$v_str = $this->quote($v[0]) . ' AND ' . $this->quote($v[1]);
				} else if (\is_array($v) && !empty($v)) {
					$v_str = null;
					foreach ($v AS $one) {
						if (\is_array($one)) {
							$sub_items = '';
							foreach ($one as $sub_value) {
								$sub_items .= ',' . $this->quote($sub_value);
							}
							$v_str .= ',(' . \substr($sub_items, 1) . ')';
						} else {
							$v_str .= ',' . $this->quote($one);
						}
					}
					$v_str = '(' . \substr($v_str, 1) . ')';
				}
				else if (empty($v)) {
					$v_str = $k;
					$v_connect = '<>';
				}
			} else {
				$v_str = $this->quote($v);
			}
			if (empty($v_connect))
				$v_connect = '=';
			$quoted_k = $this->quoteObj($k);
			if ($content)
				$content .= " $logic ( $quoted_k $v_connect $v_str ) ";
			else
				$content = " ($quoted_k $v_connect $v_str) ";
		}
		return $content;
	}
	
	
	private function quote($data, $paramType = \PDO::PARAM_STR) {
		if (\is_array($data) || \is_object($data)) {
			$return = array();
			foreach ($data as $k => $v) {
				$return [$k] = $this->quote($v);
			}
			return $return;
		} else {
			$data = $this->connection->quote($data, $paramType);
			if (false === $data)
				$data = "''";
			return $data;
		}
	}
	
	private function quoteObj($objName) {
		if (\is_array($objName)) {
			$return = array();
			foreach ($objName as $k => $v) {
				$return[] = $this->quoteObj($v);
			}
			return $return;
		} else {
			$v = \trim($objName);
			$v = \str_replace('`', '', $v);
			$v = \preg_replace('# +AS +| +#i', ' ', $v);
			$v = \explode(' ', $v);
			foreach ($v as $k_1 => $v_1) {
				$v_1 = \trim($v_1);
				if ($v_1 == '') {
					unset($v[$k_1]);
					continue;
				}
				if (strpos($v_1, '.')) {
					$v_1 = \explode('.', $v_1);
					foreach ($v_1 as $k_2 => $v_2) {
						$v_1[$k_2] = '`' . \trim($v_2) . '`';
					}
					$v[$k_1] = \implode('.', $v_1);
				} else {
					$v[$k_1] = '`' . $v_1 . '`';
				}
			}
			$v = \implode(' AS ', $v);
			return $v;
		}
	}
	
	/**
	 * 删除数据
	 * @param string $table 删除对应表的数据
	 * @param mixed $cond   条件
	 * @return number 影响行数
	 */
	public function delete($table, $cond) {
		$table = $this->quoteObj($table);
		$cond = $this->buildCondition($cond);
		$sql = "DELETE FROM {$table} WHERE $cond";
		$ret = $this->exec($sql);
		return $ret;
	}
	
	/**
	 * 插入一条数据
	 * @param  string $table   要插入的表命
	 * @param  array  $data    要插入的数据
	 * @return boolean|string 插入成功或者失败
	 */
	public function insert($table, array $params) {
		$columns = '';
		$values = '';
		foreach ($params as $column => $value) {
			$columns .= $this->quoteObj($column) . ',';
			$values .= \is_null($value) ? "NULL," : ($this->quote($value) . ',');
		}
		$columns = \substr($columns, 0, \strlen($columns) - 1);
		$values = \substr($values, 0, \strlen($values) - 1);
		
		$table = $this->quoteObj($table);
		$sql = "INSERT INTO {$table} ({$columns}) VALUES ({$values})";
		$ret = $this->exec($sql, false);
		if ($ret === false) {
			return false;
		}
		$id = $this->connection->lastInsertId();
		if (!empty($id)) {
			return $id;
		}
		return !!$ret;
	}
	
	/**
	 * 修改一条数据
	 * @param string $table  要修改数据的表
	 * @param array  $params 要修改的数据
	 * @param string $cond   修改的条件
	 * @param string $order_by_limit 排布
	 * @return boolean|number
	 */
	public function update($table, array $params, $cond, $order_by_limit = '') {
		if (empty($params))
			return false;
	
		if (\is_string($params)) {
			$update_str = $params;
		} else {
			$update_str = '';
	
			foreach ($params as $column => $value) {
				if (\is_int($column)) {
					$update_str .= "$value,";
				} else {
					$column = $this->quoteObj($column);
					$value = \is_null($value) ? 'NULL' : $this->quote($value);
					$update_str .= "{$column}={$value},";
				}
			}
			$update_str = \substr($update_str, 0, \strlen($update_str) - 1);
		}
	
		$table = $this->quoteObj($table);
		if (\is_numeric($cond))
			$cond = $this->quoteObj('id') . "='$cond'";
		else
			$cond = $this->buildCondition($cond);
		$sql = "UPDATE {$table} SET {$update_str} WHERE {$cond} {$order_by_limit}";
		$ret = $this->exec($sql);
		return $ret;
	}
	
	/**
	 * 查询字段
	 * @param string $columns
	 * @return \DB\SqlCommand
	 */
	public function select($select = '*') {
		$this->select_str = "select $select";
		return $this;
	}
	
	/**
	 * 从哪个表中查询
	 * @param unknown $table
	 * @return \DB\SqlCommand
	 */
	public function from($table) {
		$table = $this->quoteObj($table);
		$this->select_from = "FROM {$table}";
		return $this;
	}
	
	/**
	 * 传入查询条件
	 * @param array $cond
	 * @return \DB\SqlCommand
	 */
	public function where($cond = array()) {
		$cond = $this->buildCondition($cond);
		$this->select_where .= $cond ? "WHERE $cond" : '';
		return $this;
	}
	
	protected function joinInternal($join, $table, $cond) {
		$table = $this->quoteObj($table);
		$this->select_join .= " $join $table ";
		if (\is_string($cond) && (\strpos($cond, '=') === false && \strpos($cond, '<') === false && \strpos($cond, '>') === false)) {
			$column = $this->quoteObj($cond);
			$this->select_join .= " USING ($column) ";
		} else {
			$cond = $this->buildCondition($cond);
			$this->select_join .= " ON $cond ";
		}
		return $this;
	}
	
	/**
	 * 内连接
	 * @param string $table
	 * @param mixed $cond
	 * @return \DB\SqlCommand
	 */
	public function join($table, $cond) {
		return $this->joinInternal('JOIN', $table, $cond);
	}
	
	/**
	 * 左外连接
	 * @param string $table
	 * @param mixed $cond
	 * @return \DB\SqlCommand
	 */
	public function leftJoin($table, $cond) {
		return $this->joinInternal('LEFT JOIN', $table, $cond);
	}
	
	/**
	 * 右外链接
	 * @param string $table
	 * @param mixed $cond
	 * @return \DB\SqlCommand
	 */
	public function rightJoin($table, $cond) {
		return $this->joinInternal('RIGHT JOIN', $table, $cond);
	}
	
	/**
	 * 对应Mysql的group方法
	 * @param string $group
	 * @return \DB\SqlCommand
	 */
	public function group($group) {
		$this->select_group .= " GROUP BY $group ";
		return $this;
	}
	
	/**
	 * hiving语句
	 * @param mixed $cond
	 * @return \DB\SqlCommand
	 */
	public function having($cond) {
		$cond = $this->buildCondition($cond);
		$this->select_having .= " HAVING $cond ";
		return $this;
	}
	
	/**
	 * 排序
	 * @param string $order
	 * @return \DB\SqlCommand
	 */
	public function order($order) {
		$this->select_order .= " ORDER BY $order ";
		return $this;
	}
	
	/**
	 * 查询某个字段
	 * @param string $sql
	 * @param string $default
	 * @return unknown|string
	 */
	public function queryScalar($sql = null, $default = null) {
		$stmt = $this->query($sql);
		$v = $stmt->fetchColumn(0);
		if ($v !== false)
			return $v;
		return $default;
	}
	
	/**
	 * queryScalar的别名
	 * @param string $sql
	 * @param string $default
	 * @return Ambigous <\DB\unknown, string>
	 */
	public function querySimple($sql = null, $default = null) {
		return $this->queryScalar($sql, $default);
	}
	
	/**
	 * 查询出一行数据
	 * @param string $sql
	 * @return array
	 */
	public function queryRow($sql = null) {
		$stmt = $this->query($sql);
		$data = $stmt->fetch(\PDO::FETCH_ASSOC);
		return $data;
	}
	
	/**
	 * 查询满足条件的某一列的数据
	 * @param string $sql
	 * @return multitype:
	 */
	public function queryColumn($sql = null) {
		$stmt = $this->query($sql);
		$data = $stmt->fetchAll(\PDO::FETCH_COLUMN, 0);
		return $data;
	}
	
	/**
	 * 查询所有数据
	 * @param string $sql
	 * @param string $key
	 * @return array
	 */
	public function queryAll($sql = null, $key = '') {
		if ($key)
			return $this->queryAllAssocKey($sql, $key);
	
		$stmt = $this->query($sql);
		$data = $stmt->fetchAll(\PDO::FETCH_ASSOC);
		return $data;
	}
	
	/**
	 * 返回以某一列作为键的数组
	 * @param string $sql
	 * @param string $key
	 * @return array
	 */
	public function queryAllAssocKey($sql, $key) {
		$rows = array();
		$stmt = $this->query($sql);
		if ($stmt) {
			while (($row = $stmt->fetch(\PDO::FETCH_ASSOC)) !== false)
				$rows[$row[$key]] = $row;
		}
		return $rows;
	}
	
	/**
	 * 判断某条数据是否存在
	 * @param string $table 对应的表名
	 * @param mixed $cond   查询条件
	 * @return boolean      存在返回true,否则返回false
	 */
	public function exists($table, $cond) {
		$table = $this->quoteObj($table);
		$where = $this->buildWhere($cond);
		$sql = "SELECT 1 FROM $table $where LIMIT 1";
		return !!$this->querySimple($sql);
	}
	
	
	/**
	 * 对应Mysql的Limit
	 * @param integer $a
	 * @param integer $b
	 * @return \DB\SqlCommand
	 */
	public function limit($a, $b = null) {
		if (is_null($b)) {
			$a = intval($a);
			$this->select_limit .= " LIMIT $a ";
		} else {
			$a = intval($a);
			$b = intval($b);
			$this->select_limit .= " LIMIT $a, $b ";
		}
		return $this;
	}
}