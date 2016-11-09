<?php
/* 
 * stAn: this fork allows you to simply replicate MS SQL into Mysql on latest php7 using php-pdo and php-pdo_odbc (FreeTds + odbc) 
 * follow this to setup the connector http://stackoverflow.com/questions/20163776/connect-php-to-mssql-via-pdo-odbc
 * tested on php7 provided by ppa:/ondrej-php on Ubuntu 12.04, 14.04 and 16.04 https://launchpad.net/~ondrej/+archive/ubuntu/php
 * SOURCE: MS SQL
 */
//define('MSSQL_HOST','mssql_host');
define('MSSQL_SERVERNAME','servername from freeTDS config'); //section name from /etc/freetds/freetds.conf and /etc/odbc.ini
define('MSSQL_USER','mssql_user');
define('MSSQL_PASSWORD','mssql_password');
define('MSSQL_DATABASE','mssql_database');

/*
* DESTINATION: MySQL
*/
define('MYSQL_HOST', 'mysql_host');
define('MYSQL_USER', 'mysql_user');
define('MYSQL_PASSWORD','mysql_password');
define('MYSQL_DATABASE','mysql_database');

/*
 * STOP EDITING!
 */

set_time_limit(0);

function addQuote($string)
{
	return "'".$string."'";
}

function addTilde($string)
{
	return "`".$string."`";
}

// Connect MS SQL
try {
  $mssql = new PDO('odbc:DRIVER=freetds;SERVERNAME='.MSSQL_SERVERNAME.';DATABASE=' . MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD) or die("Couldn't connect to SQL Server on '".MSSQL_HOST."'' user '".MSSQL_USER."'\n");
}
catch (Exception $e) {
	echo $e."\n"; 
	die(1); 
}
echo "=> Connected to Source MS SQL Server on '".MSSQL_SERVERNAME."'\n";


// Connect to MySQL
$mysqli = new mysqli(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE);
if ($mysqli->connect_error) {
    echo 'Connect Error (' . $mysqli->connect_errno . ') '
            . $mysqli->connect_error."\n";
	die(1); 
}
else {
	echo "\n=> Connected to Source MySQL Server on ".MYSQL_HOST."\n";
}
// Select MySQL Database

$mssql_tables = array();

// Get MS SQL tables
$sql = "SELECT * FROM sys.Tables;";

$qe = $mssql->prepare($sql);
$qe->execute();

echo "\n=> Getting tables..\n";

while ($row = $qe->fetch(PDO::FETCH_ASSOC))
{
	array_push($mssql_tables, $row['name']);
	
	
	
}


// Get MS SQL Views
$sql = "SELECT * FROM sys.Views;";


$qe = $mssql->prepare($sql);
$qe->execute();

echo "\n=> Getting Views..\n";

while ($row = $qe->fetch(PDO::FETCH_ASSOC))
{
	array_push($mssql_tables, $row['name']);
	
	
	
}

echo "==> Found ". number_format(count($mssql_tables),0,',','.') ." tables\n\n";

// Get Table Structures
if (!empty($mssql_tables))
{
	$i = 1;
	foreach ($mssql_tables as $table)
	{
		echo '====> '.$i.'. '.$table."\n";
		echo "=====> Getting info table ".$table." from SQL Server\n";

		$sql = "select * from information_schema.columns where table_name = '".$table."'";
		
		
		$qe = $mssql->prepare($sql);
		$res = $qe->execute();
		
		if ($res) 
		{
			$mssql_tables[$table] = array();

			$mysql = "DROP TABLE IF EXISTS `".$table."`";
			$mysqli->query($mysql);
			$mysql = "CREATE TABLE `".$table."`";
			$strctsql = $fields = array();

			
			while ($row = $qe->fetch(PDO::FETCH_ASSOC))
			{
				//print_r($row); echo "\n";
				array_push($mssql_tables[$table], $row);

				switch ($row['DATA_TYPE']) {
					case 'bit':
					case 'tinyint':
					case 'smallint':
					case 'int':
					case 'bigint':
						$data_type = $row['DATA_TYPE'].(!empty($row['NUMERIC_PRECISION']) ? '('.$row['NUMERIC_PRECISION'].')' : '' );
						break;
					
					case 'money':
						$data_type = 'decimal(19,4)';
						break;
					case 'smallmoney':
						$data_type = 'decimal(10,4)';
						break;
					
					case 'real':
					case 'float':
					case 'decimal':
					case 'numeric':
						$data_type = $row['DATA_TYPE'].(!empty($row['NUMERIC_PRECISION']) ? '('.$row['NUMERIC_PRECISION'].(!empty($row['NUMERIC_SCALE']) ? ','.$row['NUMERIC_SCALE'] : '').')' : '' );
						break;

					case 'date':
					case 'datetime':
					case 'timestamp':
					case 'time':
						$data_type = $row['DATA_TYPE'];
					case 'datetime2':
					case 'datetimeoffset':
					case 'smalldatetime':
						$data_type = 'datetime';
						break;

					case 'nchar':
					case 'char':
						$data_type = 'char'.(!empty($row['CHARACTER_MAXIMUM_LENGTH']) && $row['CHARACTER_MAXIMUM_LENGTH'] > 0 ? '('.$row['CHARACTER_MAXIMUM_LENGTH'].')' : '(255)' );
						break;
					case 'nvarchar':
					case 'varchar':
						$data_type = 'varchar'.(!empty($row['CHARACTER_MAXIMUM_LENGTH']) && $row['CHARACTER_MAXIMUM_LENGTH'] > 0 ? '('.$row['CHARACTER_MAXIMUM_LENGTH'].')' : '(255)' );
						break;
					case 'ntext':
					case 'text':
						$data_type = 'text';
						break;

					case 'binary':
					case 'varbinary':
						$data_type = $data_type = $row['DATA_TYPE'];
					case 'image':
						$data_type = 'blob';
						break;

					case 'uniqueidentifier':
						$data_type = 'char(36)';//'CHAR(36) NOT NULL';
						break;

					case 'cursor':
					case 'hierarchyid':
					case 'sql_variant':
					case 'table':
					case 'xml':
					default:
						$data_type = false;
						break;
				}

				if (!empty($data_type))
				{
					$ssql = "`".$row['COLUMN_NAME']."` ".$data_type." ".($row['IS_NULLABLE'] == 'YES' ? 'NULL' : 'NOT NULL');
					array_push($strctsql, $ssql);
					array_push($fields, $row['COLUMN_NAME']);	
				}
				
			}

			$mysql .= "(".implode(',', $strctsql).");";
			echo "======> Creating table ".$table." on MySQL... ";
			$q = $mysqli->query($mysql);
			echo (($q) ? 'Success':'Failed!'."\n".$mysql."\n")."\n";
			
			echo "=====> Getting data from table ".$table." on SQL Server\n";
			$sql = "SELECT * FROM ".$table.' where 1=1';
			
			
			$qe = $mssql->prepare($sql);
			
			$qe->execute();
			
			$numrow = $qe->rowCount();
			echo "======> Found ".number_format($numrow,0,',','.')." rows\n";

			if ($numrow)
			{
				echo "=====> Inserting to table ".$table." on MySQL\n";
				$numdata = 0;
				if (!empty($fields))
				{
					$sfield = array_map('addTilde', $fields);
					while ($qrow = $qe->fetch(PDO::FETCH_ASSOC))
					{
						$datas = array();
						foreach ($fields as $field) 
						{
							$ddata = (!empty($qrow[$field])) ? $qrow[$field] : '';
							array_push($datas,"'".$mysqli->real_escape_string($ddata)."'");
						}

						if (!empty($datas))
						{
							
							$mysql = "INSERT INTO `".$table."` (".implode(',',$sfield).") VALUES (".implode(',',$datas).");";
							
							$q = $mysqli->query($mysql);
							$numdata += ($q ? 1 : 0 );
						}
					}
				}
				echo "======> ".number_format($numdata,0,',','.')." data inserted\n\n";
			}
		}
		$i++;
	}

}

echo "Done!\n";

$qe = null; 
$mssql = null; 
$mysqli->close();
$mysqli = null; 
