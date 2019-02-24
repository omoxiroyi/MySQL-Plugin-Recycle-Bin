# MySQL-Plugin-Recycle-Bin

## 一、简介

recycle_bin是一款MySQL插件，可以在不修改任何MySQL代码的情况下，自动备份MySQL中被Drop的表，在出现人为误操作删表时，可以快速的进行恢复，
实现思路借鉴了Oracle flashback的功能，但是从功能完整性上来讲，还有较大的差距,目前仍在完善中。

recycle_bin并不直接作用于master，而是工作在主从环境中的从机上，当通过程序或者人为在master上进行drop table操作时，slave会拦截drop操作，
先进行数据备份，再进行删除操作。当备份文件超过保存时间后，recycle_bin会自动清除这些备份数据。

欢迎加入MySQL内核交流群

![QQ群](./doc/img/recycle_bin.png)

## 二、Recycle_bin详细说明

### 编译/下载/安装/卸载
可以通过下载源码编译，或者直接下载库文件

#### 源码编译
源码编译依赖MySQL源代码，推荐使用5.7.18以上的版本进行编译。
```sh
git clone git@github.com:sunashe/MySQL-Plugin-Recycle-Bin.git
cp -r MySQL-Plugin-Recycle-Bin  mysql_source_dir/plugin/
cd mysql_source_dir
cmake . -DBUILD_CONFIG=mysql_release -DDOWNLOAD_BOOST=1  -DWITH_BOOST=/usr/local/boost/
cd plugin/MySQL-Plugin-Recycle-Bin
make 
#拷贝当前目录下的recycle_bin.so到MySQL中配置的plugin_dir下。
```

#### 下载编译好的lib文件
```bash
下载页面
https://github.com/sunashe/MySQL-Plugin-Recycle-Bin/releases
```

#### 安装插件
```bash
mysql> install plugin recycle_bin soname 'recycle_bin.so';
Query OK, 0 rows affected (0.02 sec)
```
查看MySQL错误日志，如果出现如下内容，则证明安装成功
```bash
2019-02-20T19:52:27.326951+08:00 22 [Note] Install Plugin 'recycle_bin' successfully.
```

#### 卸载插件
```bash
mysql> uninstall plugin recycle_bin;
Query OK, 0 rows affected (0.02 sec)
```
### 参数说明

- recycle_bin_enabled
全局控制操作，ON表示开启recycle_bin的功能，在recycle_bin准备就绪后，会自动备份被Drop的表

- recycle_bin_database_name
指定回收站库名，默认为recycle_bin_dba

- recycle_bin_expire_seconds
备份表的过期时间，或者较保留时间，默认为1800s

- recycle_bin_check_sql_delay_period
备份操作时，需要等待所有相关的dml语句回放完成，recycle_bin_check_sql_delay_period用于
控制判断周期。默认为10us.

### 状态值

- Recycle_bin_status
recycle_bin的工作状态，recycle_bin_enabled只是一个参数控制，而Recycle_bin_status表示已经符合了条件
，正式开始工作。一般只有在接收到master的FD event后才会正常工作。所以最好在安装插件后，在master上执行一次flush logs操作
或者重启复制IO线程。


#### 使用示例
slave查看当前回收站状态
```bash
mysql> show global status like '%recycle%';
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| Recycle_bin_status | ON    |
+--------------------+-------+
```
master上新建测试库表，插入数据
```bash
mysql> use sunashe;
Database changed
mysql> create table t1(id int not null auto_increment primary key,name varchar(10))
    -> ;
Query OK, 0 rows affected (0.02 sec)

mysql> insert into t1(name) values('aaa');
Query OK, 1 row affected (0.00 sec)

mysql> insert into t1(name) values('aaa');
Query OK, 1 row affected (0.00 sec)

mysql> insert into t1(name) values('aaa');
Query OK, 1 row affected (0.00 sec)

mysql> select * from t1;
+----+------+
| id | name |
+----+------+
|  1 | aaa  |
|  2 | aaa  |
|  3 | aaa  |
+----+------+
3 rows in set (0.00 sec)
```
slave上查看同步的数据
```bash
mysql> use sunashe;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> show tables;
+-------------------+
| Tables_in_sunashe |
+-------------------+
| t1                |
+-------------------+
1 row in set (0.01 sec)

mysql> select * from t1;
+----+------+
| id | name |
+----+------+
|  1 | aaa  |
|  2 | aaa  |
|  3 | aaa  |
+----+------+
3 rows in set (0.00 sec)
```

master上执行drop table操作
```bash
mysql> drop table t1;
Query OK, 0 rows affected (0.00 sec)
mysql> show tables;
Empty set (0.00 sec)

```
此时slave中sunashe库下的t1表也被删除了，但是存放在了回收站库中。
```bash
mysql> show tables;
Empty set (0.00 sec)

mysql> show tables in recycle_bin_dba;
+-------------------------------------+
| Tables_in_recycle_bin_dba           |
+-------------------------------------+
| db1_t1_ashesun_1550664699603202     |
| sunashe_t1_ashesun_1550664941582282 |
+-------------------------------------+

//错误日志
2019-02-20T20:15:41.581959+08:00 23 [Note] Master drop table sunashe.t1
2019-02-20T20:15:41.610112+08:00 23 [Note] Backup table sunashe.t1 successfully.
2 rows in set (0.00 sec)

```
表名格式为原库名_原表名_时间flag_unix时间戳(us)，查询备份数据正确性
```bash
mysql> select * from recycle_bin_dba.sunashe_t1_ashesun_1550664941582282;
+----+------+
| id | name |
+----+------+
|  1 | aaa  |
|  2 | aaa  |
|  3 | aaa  |
+----+------+
3 rows in set (0.01 sec)
```
备份数据过期后，在业务低峰期时(收到心跳event),自动清除。关联参数recycle_bin_expire_seconds.
```bash
mysql> set global recycle_bin_expire_seconds=10;
Query OK, 0 rows affected (0.01 sec)

mysql> show tables in recycle_bin_dba;
+-------------------------------------+
| Tables_in_recycle_bin_dba           |
+-------------------------------------+
| db1_t1_ashesun_1550664699603202     |
| sunashe_t1_ashesun_1550664941582282 |
+-------------------------------------+
2 rows in set (0.00 sec)

mysql> show tables in recycle_bin_dba;
Empty set (0.00 sec)

```
查看复制环境是否正常运行
```bash
mysql> show slave status\G
*************************** 1. row ***************************
               Slave_IO_State: Waiting for master to send event
                  Master_Host: 10.211.55.32
                  Master_User: ashe
                  Master_Port: 13307
                Connect_Retry: 60
              Master_Log_File: mysql-bin.000003
          Read_Master_Log_Pos: 3080
               Relay_Log_File: mysql-relay-bin-master_13307.000004
                Relay_Log_Pos: 3293
        Relay_Master_Log_File: mysql-bin.000003
             Slave_IO_Running: Yes
            Slave_SQL_Running: Yes
              Replicate_Do_DB:
          Replicate_Ignore_DB:
           Replicate_Do_Table:
       Replicate_Ignore_Table: mysql.ibbackup_binlog_marker
      Replicate_Wild_Do_Table:
  Replicate_Wild_Ignore_Table: mysql.backup_%
                   Last_Errno: 0
                   Last_Error:
                 Skip_Counter: 0
          Exec_Master_Log_Pos: 3080
              Relay_Log_Space: 3600
              Until_Condition: None
               Until_Log_File:
                Until_Log_Pos: 0
           Master_SSL_Allowed: No
           Master_SSL_CA_File:
           Master_SSL_CA_Path:
              Master_SSL_Cert:
            Master_SSL_Cipher:
               Master_SSL_Key:
        Seconds_Behind_Master: 0
Master_SSL_Verify_Server_Cert: No
                Last_IO_Errno: 0
                Last_IO_Error:
               Last_SQL_Errno: 0
               Last_SQL_Error:
  Replicate_Ignore_Server_Ids:
             Master_Server_Id: 12713307
                  Master_UUID: cdfe45e6-c227-11e8-abf5-001c42bf9720
             Master_Info_File: mysql.slave_master_info
                    SQL_Delay: 0
          SQL_Remaining_Delay: NULL
      Slave_SQL_Running_State: Slave has read all relay log; waiting for more updates
           Master_Retry_Count: 86400
                  Master_Bind:
      Last_IO_Error_Timestamp:
     Last_SQL_Error_Timestamp:
               Master_SSL_Crl:
           Master_SSL_Crlpath:
           Retrieved_Gtid_Set: cdfe45e6-c227-11e8-abf5-001c42bf9720:708-1419
            Executed_Gtid_Set: cdfe45e6-c227-11e8-abf5-001c42bf9720:1-1419
                Auto_Position: 1
         Replicate_Rewrite_DB:
                 Channel_Name: master_13307
           Master_TLS_Version:
1 row in set (0.01 sec)
```
recycle_bin的备份清除等操作不会记录任何binlog信息，保证和master的GTID一致。

## 其它使用说明

>* 复制必须开启GTID，即MySQL参数`gtid_mode=on`
>* 支持异步或者半同步复制，对于异步复制来讲，不会影响master的性能，对于半同步复制来讲，对master有5%左右的性能损失。
>* 可以通过手动drop table的方式清除recycle_bin_dba下的备份表。
>* 由于备份表的表名格式为原库名_原表名_时间flag_unix时间戳(us)，所以库名+表名最长不能超过（64-16(us)-7(flag)）
