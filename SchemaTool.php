<?php

/**
 * Created by PhpStorm.
 * User: mofan
 * Date: 2016/1/20 0020
 * Time: 10:28
 */


class SchemaTool {

	static $verbose = 1;
	const COPY_PAGE_SIZE = 5000;

	static public function addField($tableCreateSql, $schema, mysqli $mysqli) {

		set_time_limit(99999999);


		$table = '';
		if (!self::runVerbose('parse create sql',
			function()use($tableCreateSql, &$schema, &$table){
				if (!self::parseCreateSql($tableCreateSql, $schema, $table)) {
					self::debugIndent("can not parse schema and table from given create sql");
					return false;
				}
				return true;
			}
		)) {
			return false;
		}


		if (!self::runVerbose("check no table named `$schema`.`{$table}_old`",
			function()use($mysqli, $schema, $table){
				if (!$mysqli->real_query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE
					TABLE_SCHEMA='$schema' AND TABLE_NAME='{$table}_old'")
				) {
					self::debugIndent($mysqli->error);
					return false;
				}
				$result = $mysqli->store_result();
				$row = $result->fetch_assoc();
				if ($row) {
					self::debugIndent("table `$schema`.`{$table}_old` exist");
					return false;
				}
				return true;
			}
		)) {
			return false;
		}


		$oldPKField = '';
		$oldFields = array();
		if (!self::runVerbose('read old table fields',
			function()use($mysqli, $schema, $table, &$oldFields, &$oldPKField){
				if (!$mysqli->real_query("DESCRIBE `$schema`.`$table`")){
					self::debugIndent($mysqli->error);
					return false;
				}
				$result = $mysqli->store_result();
				while ($row = $result->fetch_assoc()) {
					if ($oldPKField=='' && $row['Key']=='PRI') {
						$oldPKField = $row['Field'];
					}
					$oldFields[] = $row['Field'];
				}
				if (empty($oldFields)) {
					self::debugIndent("can not get fields in $schema.$table");
					return false;
				}
				self::debugIndent("found primary key $oldPKField");
				return true;
			}
		)) {
			return false;
		}


		$tableNew = $table.'_new';


		$tableCreateSql = str_replace($table, $tableNew, $tableCreateSql);
		if (!self::runVerbose('create new table',
			function()use($mysqli, $tableCreateSql, $schema){
				if (!$mysqli->select_db($schema)) {
					self::debugIndent("can not select db: {$mysqli->error}");
					return false;
				}
				if (!$mysqli->real_query($tableCreateSql)) {
					self::debugIndent("create table failed: {$mysqli->error}");
					return false;
				}
				return true;
			}
		)) {
			return false;
		}


		$newPKField = '';
		$newFields = array();
		if (!self::runVerbose('read new table fields',
			function()use($mysqli, $schema, $tableNew, &$newFields, &$newPKField){
				if (!$mysqli->real_query("DESCRIBE `$schema`.`$tableNew`")) {
					self::debugIndent($mysqli->error);
					return false;
				}
				$result = $mysqli->store_result();
				while ($row = $result->fetch_assoc()) {
					if ($newPKField=='' && $row['Key']=='PRI') {
						$newPKField = $row['Field'];
					}
					$newFields[] = $row['Field'];
				}
				if (empty($newFields)) {
					self::debugIndent("can not get fields of $schema.$tableNew");
					return false;
				}
				return true;
			}
		)) {
			self::removeUnuseTable($schema, $tableNew, $mysqli);
			return false;
		}


		if (!self::runVerbose('check primary key are same',
			function()use($mysqli, $schema, $tableNew, $newPKField, $oldPKField){
				if ($newPKField==''||$oldPKField==''||$newPKField!=$oldPKField) {
					return false;
				}
				return true;
			}
		)) {
			self::removeUnuseTable($schema, $tableNew, $mysqli);
			return false;
		}


		if (!self::runVerbose('check no fields missing',
			function()use($oldFields, $newFields, $schema, $tableNew, $mysqli){
				$arr = array_diff($oldFields, $newFields);
				if (!empty($arr)) {
					self::debugIndent("some fields missing");
					return false;
				}
				return true;
			}
		)) {
			self::removeUnuseTable($mysqli, $schema, $tableNew);
			return false;
		}


		if (!self::runVerbose("create trigger `$schema`.`after_{$table}_insert`",
			function()use($mysqli, $schema, $table, $tableNew, $oldFields){
				$sql = "CREATE TRIGGER `$schema`.`after_{$table}_insert` AFTER insert ";
				$sql .= "ON `$schema`.`$table` ";
				$sql .= "FOR EACH ROW BEGIN ";
				$sql .= "replace into `$schema`.`$tableNew` ";
				$sql .= "(`";
				$sql .= implode('`,`', $oldFields);
				$sql .= "`)";
				$sql .= "values";
				$sql .= "(";
				$sql .= implode(',', array_map(function($s){return 'new.'.$s;}, $oldFields));
				$sql .= ");";
				$sql .= "END";
				if (!$mysqli->real_query($sql)) {
					self::debugIndent($mysqli->error);
					return false;
				}
				return true;
			}
		)) {
			self::removeUnuseTable($mysqli, $schema, $tableNew);
			return false;
		}


		if (!self::runVerbose("create trigger `$schema`.`after_{$table}_update`",
			function()use($mysqli, $schema, $table, $tableNew, $oldFields){
				$sql = "CREATE TRIGGER `$schema`.`after_{$table}_update` AFTER update ";
				$sql .= "ON `$schema`.`$table` ";
				$sql .= "FOR EACH ROW BEGIN ";
				$sql .= "replace into `$schema`.`$tableNew` ";
				$sql .= "(`";
				$sql .= implode('`,`', $oldFields);
				$sql .= "`)";
				$sql .= "values";
				$sql .= "(";
				$sql .= implode(',', array_map(function($s){return 'new.'.$s;}, $oldFields));
				$sql .= ");";
				$sql .= "END";
				if (!$mysqli->real_query($sql)) {
					self::debugIndent($mysqli->error);
					return false;
				}
				return true;
			}
		)) {
			self::removeTriggers($mysqli, $schema, $table);
			self::removeUnuseTable($mysqli, $schema, $tableNew);
			return false;
		}


		$currentMaxPkValue = '';
		if (!self::runVerbose("get current max pk value",
			function()use($mysqli, $oldPKField, $schema, $table, $tableNew, &$currentMaxPkValue){
				if (!$mysqli->real_query("select count(*) as cnt from `$schema`.`$table`")) {
					self::debugIndent($mysqli->error);
					return false;
				}
				$result = $mysqli->store_result();
				$row = $result->fetch_assoc();
				if ($row['cnt'] == 0) {
					self::debugIndent("nothing to copy");
					return true;
				}
				if (!$mysqli->real_query("select $oldPKField from `$schema`.`$table` order by $oldPKField desc limit 1")) {
					self::debugIndent($mysqli->error);
					return false;
				}
				$result = $mysqli->store_result();
				$row = $result->fetch_assoc();
				if (!$row) {
					self::debugIndent("$currentMaxPkValue not found");
					return false;
				}
				$currentMaxPkValue = $row[$oldPKField];
				return true;
			}
		)) {
			self::removeTriggers($mysqli, $schema, $table);
			self::removeUnuseTable($mysqli, $schema, $tableNew);
			return false;
		}


		if (!$currentMaxPkValue) { // nothing to copy
			self::removeTriggers($mysqli, $schema, $table);
			self::removeUnuseTable($mysqli, $schema, $tableNew);
			return true;
		}


		self::debug("got current max pk value: $currentMaxPkValue");


		if (!self::runVerbose("copy data",
			function()use($mysqli, $currentMaxPkValue, $schema, $table, $tableNew, $oldFields, $oldPKField){
				if (!$mysqli->real_query("select count(*) as cnt from `$schema`.`$table`")) {
					self::debugIndent($mysqli->error);
					return false;
				}
				$result = $mysqli->store_result();
				$row = $result->fetch_assoc();
				if (!$row) {
					self::debugIndent($mysqli->error);
					return false;
				}
				$total = $row['cnt'];
				$pageSize = self::COPY_PAGE_SIZE;
				self::debugIndent("total rows: $total");
				$strFields = implode(',', $oldFields);
				$maxPKV = $currentMaxPkValue;
				while(1) {
					if (!$mysqli->real_query("SELECT MIN(A.$oldPKField) as minpkv, MAX(A.$oldPKField) as maxpkv FROM
						(SELECT $oldPKField FROM `$schema`.`$table` WHERE $oldPKField<=$maxPKV ORDER BY $oldPKField DESC
						LIMIT $pageSize) AS A")
					) {
						self::debugIndent($mysqli->error);
						return false;
					}
					if (!$result = $mysqli->store_result()) {
						self::debugIndent($mysqli->error);
						return false;
					}
					if (!$row = $result->fetch_assoc()) {
						break;
					}
					if (!$mysqli->real_query("insert ignore into `$schema`.`$tableNew`($strFields)
						select $strFields from `$schema`.`$table` where $oldPKField between {$row['minpkv']} and {$row['maxpkv']}")
					) {
						self::debugIndent($mysqli->error);
						return false;
					}
					if ($row['minpkv'] == $row['maxpkv']) {
						break;
					}

					$maxPKV = $row['minpkv'];

					$total -= $mysqli->affected_rows;
					self::debugIndent('remain rows: '. $total);
					usleep(1000);//稍微歇一会儿，给别人点机会
				}
				return true;
			}
		)) {
			self::removeTriggers($mysqli, $schema, $table);
			self::removeUnuseTable($mysqli, $schema, $tableNew);
			return false;
		}


		if (!self::runVerbose("check data rows",
			function()use($mysqli, $schema, $table, $tableNew, $oldPKField, $currentMaxPkValue){
				if (!$mysqli->real_query("select count(*) as cnt from `$schema`.`$table` where $oldPKField<=$currentMaxPkValue")) {
					self::debugIndent($mysqli->error);
					return false;
				}
				$result = $mysqli->store_result();
				$row = $result->fetch_assoc();
				$oldRows = $row['cnt'];
				if (!$mysqli->real_query("select count(*) as cnt from `$schema`.`$tableNew` where $oldPKField<=$currentMaxPkValue")) {
					self::debugIndent($mysqli->error);
					return false;
				}
				$result = $mysqli->store_result();
				$row = $result->fetch_assoc();
				if ($row['cnt']!=$oldRows || $oldRows<1) {
					self::debugIndent("rows count where $oldPKField<=$currentMaxPkValue are different $oldRows||{$row['cnt']}");
					return false;
				}
				return true;
			}
		)) {
			self::removeTriggers($mysqli, $schema, $table);
			self::removeUnuseTable($mysqli, $schema, $tableNew);
			return false;
		}


		if (!self::runVerbose("rename $table=>{$table}_old,$tableNew=>$table",
			function()use($mysqli, $schema, $table, $tableNew){
				if (!$mysqli->real_query(
					"RENAME TABLE
				 	`$schema`.`$table` TO `$schema`.`{$table}_old`,
				 	`$schema`.`$tableNew` TO `$schema`.`$table`"
				)) {
					self::debugIndent($mysqli->error);
					return false;
				}
				return true;
			}
		)) {
			return false;
		}


		if (!self::runVerbose("clean up",
			function()use($mysqli, $schema, $table, $tableNew){
				self::removeTriggers($mysqli, $schema, $table);
				self::removeUnuseTable($mysqli, $schema, $tableNew);
				return true;
			}
		)) {
			return false;
		}


		self::debug("all work well done!!!");
		return true;
	}

	static public function removeTriggers(mysqli $mysqli, $schema, $table) {
		if($mysqli->real_query("drop trigger IF EXISTS `$schema`.`after_{$table}_insert`")){
			self::debugIndent("trigger `$schema`.`after_{$table}_insert` removed");
		}
		if($mysqli->real_query("drop trigger IF EXISTS `$schema`.`after_{$table}_update`")){
			self::debugIndent("trigger `$schema`.`after_{$table}_update` removed");
		}
	}

	static public function removeUnuseTable(mysqli $mysqli, $schema, $tableNew) {
		if (self::dropTable($schema, $tableNew, $mysqli)) {
			self::debugIndent("table `$schema`.`$tableNew` droped");
		}
	}

	static protected function runVerbose($doing, $cb) {
		self::debug($doing.	   '  [ start ]');
		$ret = $cb();
		if ($ret===false) {
			self::debug($doing.'  [ failed ]');
		}else{
			self::debug($doing.'  [ ok ]');
		}
		return $ret;
	}

	static protected function debug($msg) {
		if(self::$verbose) {
			echo trim($msg)."\n";
		}
	}

	static protected function debugIndent($msg) {
		if (self::$verbose) {
			echo '    '.trim($msg)."\n";
		}
	}

	static public function dropTable($schema, $table, mysqli $mysqli) {
		return $mysqli->real_query("DROP TABLE IF EXISTS `$schema`.`$table`");
	}

	static public function parseCreateSql($tableCreateSql, &$schema, &$table) {
		if (!preg_match('/CREATE\s+TABLE\s+([\w\.\`]+)/is', $tableCreateSql, $m)) {
			return false;
		}

		$str = str_replace('`', '', $m[1]);
		$arr = array_filter(explode('.', $str));
		if (empty($arr)) {
			return false;
		}

		if (isset($arr[1])) {
			$schema = $arr[0];
			$table = $arr[1];
		}else{
			$table = $arr[0];
		}

		return true;
	}

	/**
	 * @param mysqli $mysqli
	 * @return array|false
	 */
	static public function getAllDBSize(mysqli $mysqli) {
		$sql = "SELECT
			TABLE_SCHEMA AS db_name,
			CONCAT(TRUNCATE(SUM(data_length)/1024/1024,2),' MB') AS data_size,
			CONCAT(TRUNCATE(SUM(index_length)/1024/1024,2),'MB') AS index_size
		FROM information_schema.tables
		GROUP BY TABLE_SCHEMA
		ORDER BY data_length DESC";
		if (!$mysqli->real_query($sql)) {
			if (self::$verbose) {
				echo 'getAllDBSize failed:'. $mysqli->error ."\n";
			}
			return false;
		}
		$rs = array();
		$result = $mysqli->store_result();
		while($row = $result->fetch_assoc()) {
			$rs[] = $row;
		}

		return $rs;
	}

	static public function getDBSize($dbname, mysqli $mysqli) {
		$sql = "SELECT
			TABLE_SCHEMA AS db_name,
			CONCAT(TRUNCATE(SUM(data_length)/1024/1024,2),' MB') AS data_size,
			CONCAT(TRUNCATE(SUM(index_length)/1024/1024,2),'MB') AS index_size
		FROM information_schema.tables WHERE TABLE_SCHEMA='$dbname'
		GROUP BY TABLE_SCHEMA
		ORDER BY data_length DESC";
		if (!$mysqli->real_query($sql)) {
			if (self::$verbose) {
				echo 'getDBSize failed:'. $mysqli->error ."\n";
			}
			return false;
		}

		$result = $mysqli->store_result();
		if ($row = $result->fetch_assoc()) {
			return $row;
		}

		if (self::$verbose) {
			echo "Schema $dbname not exist\n";
		}

		return false;
	}

}


///////////////////////main/////////////////////////
if (!isset($argv[4])) {
	echo $argv[0].' host user psw path2createSqlFile';
	exit(1);
}
$host = $argv[1];
$user = $argv[2];
$psw = $argv[3];
$sqlFile = $argv[4];


$mysqli = new mysqli();
$mysqli->real_connect($host, $user, $psw, null, null);

$createSql = rtrim(file_get_contents($sqlFile), ';');
$r = SchemaTool::addField($createSql, 'testa', $mysqli);
$mysqli->close();


exit($r?0:1);