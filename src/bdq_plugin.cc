/* Copyright (C) 2007 Google Inc.
   Copyright (C) 2008 MySQL AB
   Copyright (c) 2008, 2015, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */


#include "bdq_slave.h"
#include <mysql.h>
#include <mysqld_error.h>
#include <sql_thd_internal_api.h>
#include <auth/sql_acl.h>
#include <sql_tablespace.h>
#include <sql_base.h>
#include "log_event.h"
#include "sql_parse.h"
#include "conn_handler/channel_info.h"
#include "sql_rename.h"
#include "sql_db.h"
#include "sql_table.h"


void switch_recycle_bin_status(char in_status);

bdqSlave bdq_slave;
static Format_description_log_event*  glob_description_event = NULL;
static my_bool opt_verify_binlog_checksum= TRUE;
static my_bool received_fde = FALSE;
static THD* bdq_backup_thd = NULL;
static char query_create_table[]="CREATE TABLE  (id int not null auto_increment primary key)ENGINE=INNODB";
static const char query_create_backup_database[] = "create database if not exists recycle_bin_bdq";

/**
 * recycle bin所有对于数据库内部的修改都是通过IO线程来完成的。此函数用于在执行某个操作前的线程环境准备。
 * @param bdq_backup_thd
 * @param query
 * @param db
 * @param table_name
 * @return true ok; false error
 */
my_bool bdq_prepare_execute_command(THD* bdq_backup_thd,const char* query,const char* db,const char* table_name)
{

  Parser_state parser_state;
  struct st_mysql_const_lex_string new_db;
  new_db.str=db;
  new_db.length = strlen(db);
  bdq_backup_thd->reset_db(new_db);
  alloc_query(bdq_backup_thd, query,strlen(query));

  if(parser_state.init(bdq_backup_thd, bdq_backup_thd->query().str, bdq_backup_thd->query().length))
  {
    return FALSE;
  }
  mysql_reset_thd_for_next_command(bdq_backup_thd);
  lex_start(bdq_backup_thd);
  bdq_backup_thd->m_parser_state= &parser_state;
  bdq_backup_thd->m_parser_state= NULL;
  bool err= bdq_backup_thd->get_stmt_da()->is_error();

  err=parse_sql(bdq_backup_thd, &parser_state, NULL);

  return !err;
}

/**
 * 用于在执行操作后，释放相关的锁，关闭打开的表。
 * @param bdq_backup_thd
 */
void bdq_after_execute_command(THD* bdq_backup_thd)
{
  bdq_backup_thd->mdl_context.release_statement_locks();
  bdq_backup_thd->mdl_context.release_transactional_locks();
  bdq_backup_thd->lex->unit->cleanup(true);
  close_thread_tables(bdq_backup_thd);
  bdq_backup_thd->end_statement();
  bdq_backup_thd->cleanup_after_query();
}


/**
 * 在遇到表删除时，选择将被删除的表rename为指定库下的表，再新建表，用于sql线程进行真正的删除。
 * @param table_dropped_name
 * @param db
 * @param backup_dir
 * @param bdq_backup_thd
 * @return
*/
my_bool bdq_backup_table_routine(const char* table_dropped_name,const char* db,const char* backup_dir,THD* bdq_backup_thd)
{
  char* query_rename_table = new char[strlen("RENAME TABLE %s.%s to recycle_bin_bdq.%s_%s")+
                                      (strlen(db)+strlen(table_dropped_name))*2];
  char* query_create_virtual_table = new char[strlen(query_create_table) + strlen(db) + strlen(table_dropped_name)];
  int res = FALSE;

  //stage 1.改变当前线程的sql_log_bin = 0;不写binlog操作。
  if (bdq_backup_thd->variables.sql_log_bin)
    bdq_backup_thd->variables.option_bits |= OPTION_BIN_LOG;
  else
    bdq_backup_thd->variables.option_bits &= ~OPTION_BIN_LOG;

  //stage 2.create database if not exists.
  if(bdq_prepare_execute_command(bdq_backup_thd,
                                 query_create_backup_database,db,table_dropped_name))
  {
    LEX  *const lex= bdq_backup_thd->lex;
    HA_CREATE_INFO create_info(lex->create_info);
    char *alias;
    if (!(alias=bdq_backup_thd->strmake(lex->name.str, lex->name.length)) ||
        (check_and_convert_db_name(&lex->name, FALSE) != IDENT_NAME_OK))
    {

    }
    res= mysql_create_db(bdq_backup_thd,(lower_case_table_names == 2 ? alias :
                                             lex->name.str), &create_info, 0);
    bdq_after_execute_command(bdq_backup_thd);

    if(res)
    {
      sql_print_error("Create backup database error");
      goto exit_bdq_btr;
    }
  }
  else
  {
    goto exit_bdq_btr;
  }

  //stage 3.rename A.a to B.a.back.timestamp
  sprintf(query_rename_table,"RENAME TABLE %s.%s to recycle_bin_bdq.%s_%s",db,table_dropped_name,db,table_dropped_name);
  if(bdq_prepare_execute_command(bdq_backup_thd,query_rename_table,db,table_dropped_name))
  {
    LEX  *const lex= bdq_backup_thd->lex;
    /* first SELECT_LEX (have special meaning for many of non-SELECTcommands) */
    SELECT_LEX *const select_lex= lex->select_lex;
    /* first table of first SELECT_LEX */
    TABLE_LIST *const first_table= select_lex->get_table_list();
    if (mysql_rename_tables(bdq_backup_thd, first_table, 0))
    {
      bdq_after_execute_command(bdq_backup_thd);
      sql_print_error("Backup table in rename stage failed");
      goto exit_bdq_btr;
    }
    bdq_after_execute_command(bdq_backup_thd);
  }
  else
  {
    goto exit_bdq_btr;
  }

  //stage 4.create A.a(id int not null auto_increment primary key);
  sprintf(query_create_virtual_table,
          "CREATE TABLE %s.%s(id int not null auto_increment primary key)ENGINE=INNODB",db,table_dropped_name);
  if(bdq_prepare_execute_command(bdq_backup_thd,query_create_virtual_table,db,table_dropped_name))
  {
    res= FALSE;
    LEX  *const lex= bdq_backup_thd->lex;
    /* first SELECT_LEX (have special meaning for many of non-SELECTcommands) */
    SELECT_LEX *const select_lex= lex->select_lex;
    /* first table of first SELECT_LEX */
    TABLE_LIST *const first_table= select_lex->get_table_list();

    TABLE_LIST *create_table= first_table;
    TABLE_LIST *select_tables= lex->create_last_non_select_table->next_global;
    HA_CREATE_INFO create_info(lex->create_info);
    Alter_info alter_info(lex->alter_info, bdq_backup_thd->mem_root);
    if ((res= create_table_precheck(bdq_backup_thd, select_tables, create_table)))
    {
      sql_print_error("Backup table failed in create new table stage[precheck]");
      goto exit_bdq_btr;
    }

    /* Might have been updated in create_table_precheck */
    create_info.alias= create_table->alias;

    if (create_info.tablespace)
    {
      if (check_tablespace_name(create_info.tablespace) != IDENT_NAME_OK)
      {
        sql_print_error("Backup table failed in create new table stage[check_tablespace_name]");
        goto exit_bdq_btr;
      }

      if (!bdq_backup_thd->make_lex_string(&create_table->target_tablespace_name,
                                create_info.tablespace,
                                strlen(create_info.tablespace), false))
      {
        sql_print_error("Backup table failed in create new table stage[make_lex_string]");
        goto exit_bdq_btr;
      }

    }
    res= mysql_create_table(bdq_backup_thd, create_table,
                            &create_info, &alter_info);
    bdq_after_execute_command(bdq_backup_thd);
    if(res)
    {
      sql_print_error("Backup table failed in create new table stage[mysql_create_table]");
      goto exit_bdq_btr;
    }
  }
  else
  {
    goto exit_bdq_btr;
  }

  //stage 5.increment backup tables successfully status.

  exit_bdq_btr:
  bdq_after_execute_command(bdq_backup_thd);
  delete[] query_rename_table;
  delete[] query_create_virtual_table;

  return TRUE;
}


/**
 * 备份入口函数。
 * @param drop_query
 * @param db
 * @param backup_dir
 * @return
*/
my_bool bdq_backup(const char* drop_query,const char* db,const char* backup_dir)
{
  bdq_backup_thd = (THD*)my_get_thread_local(THR_THD);
  Parser_state parser_state;
  struct st_mysql_const_lex_string new_db;
  new_db.str=db;
  new_db.length = strlen(db);
  bdq_backup_thd->reset_db(new_db);

  alloc_query(bdq_backup_thd, drop_query,strlen(drop_query));
  if(parser_state.init(bdq_backup_thd, bdq_backup_thd->query().str, bdq_backup_thd->query().length))
  {
    return FALSE;
  }

  mysql_reset_thd_for_next_command(bdq_backup_thd);
  lex_start(bdq_backup_thd);
  bdq_backup_thd->m_parser_state= &parser_state;
  bdq_backup_thd->m_parser_state= NULL;
  LEX* lex= bdq_backup_thd->lex;
  bool err= bdq_backup_thd->get_stmt_da()->is_error();

  err=parse_sql(bdq_backup_thd, &parser_state, NULL);
  if(err)
  {
    sql_print_error("Plugin bdq parse_sql error");
    return FALSE;
  }

  if(!(lex->query_tables))
  {
    return TRUE;
  }
  char* in_db = strdup(lex->query_tables->db);
  lex_end(lex);

  switch(lex->sql_command)
  {
    case SQLCOM_DROP_TABLE:
    {
      char* in_table = strdup(lex->query_tables->table_name);
      sql_print_information("Master drop table %s.%s",in_db,in_table);
      if(bdq_backup_table_routine(in_table,in_db,backup_dir,bdq_backup_thd))
      {
        sql_print_information("Backup table %s.%s successfully.",in_db,in_table);
      }
      else
      {
        sql_print_error("Backup table %s.%s failed",in_db,in_table);
      }
      bdq_after_execute_command(bdq_backup_thd);
      break;
    }

    case SQLCOM_DROP_DB:
    {
      sql_print_information("Master drop database %s",db);
      break;
    }

    default:
    {
      break;
    }
  }


  return TRUE;
}


C_MODE_START

int bdq_reset_slave(Binlog_relay_IO_param *param)
{
  //Nothing to do.
  return 0;
}

int bdq_request_dump(Binlog_relay_IO_param *param,
				 uint32 flags)
{
  return 0;
}



int bdq_read_event(Binlog_relay_IO_param *param,
			       const char *packet, unsigned long len,
			       const char **event_buf, unsigned long *event_len)
{
  Log_event *ev= NULL;
  Log_event_type type= binary_log::UNKNOWN_EVENT;
  bool maybe_should_bk = false;
  const char* tmp_event_buf;
  unsigned long tmp_event_len;
  const char *error_msg= NULL;
  int i=0;
  int len_query = 0;

  if(!recycle_bin_enabled) //如果全局参数没有开启，则直接返回。提高性能。
  {
    goto exit_read_event;
  }

  bdq_slave.semisync_event(packet, len,
                                          &tmp_event_buf, &tmp_event_len);

  type = (Log_event_type)tmp_event_buf[EVENT_TYPE_OFFSET];

  if(!received_fde) //还没有收到过FORMAT_DESCRIPTION_EVENT，进行检测。
  {
    if(type == binary_log::FORMAT_DESCRIPTION_EVENT)
    {
      if (!(ev= Log_event::read_log_event((const char*)tmp_event_buf ,
                                          tmp_event_len, &error_msg,
                                          glob_description_event,
                                          opt_verify_binlog_checksum)))
      {
        sql_print_error("Plugin recycle_bin could not construct log event object: %s", error_msg);
        goto exit_read_event;
      }
      delete glob_description_event;
      glob_description_event= (Format_description_log_event*) ev;
      received_fde = TRUE;
      bdq_slave.setRecycleBinEnabled(received_fde);
      switch_recycle_bin_status('1');
    }
    else
    {
      //在从来没收到过FDE时，其它所有的binlog event都不做检测，直接返回。
      //意味着bdq的功能只有重启复制，或者master flush logs之后才会生效。
      goto exit_read_event;
    }
  }

  //已经收到了FORMAT_DESCRIPTION_EVENT，再对event type进行判断,是否可能需要备份。
  if(type == binary_log::QUERY_EVENT)
  {
    if (!(ev= Log_event::read_log_event((const char*)tmp_event_buf ,
                                        tmp_event_len, &error_msg,
                                        glob_description_event,
                                        opt_verify_binlog_checksum)))
    {
      sql_print_error("Could not construct log event object: %s", error_msg);
    }
    Query_log_event* qle = dynamic_cast<Query_log_event*>(ev);
    binary_log::Query_event* qe = new Query_log_event;
    qe = qle;


    //find substr "DROP",that maybe should bk.
    len_query=strlen(qe->query);
    for(i=0;i<len_query;i++)
    {
      if(strncasecmp(qe->query+i,"DROP",4) ==0 )
      {
        maybe_should_bk = true;
        break;
      }
    }

    if(!maybe_should_bk)
    {
      goto exit_read_event;
    }
    bdq_backup(qe->query,qe->db,home_dir);
  }
  else //其它非binary_log::QUERY_EVENT的Drop行为，有吗？
  {
    goto exit_read_event;
  }

  exit_read_event:
  delete ev;
  ev=NULL;

  return 0;
}

int bdq_queue_event(Binlog_relay_IO_param *param,
				const char *event_buf,
				unsigned long event_len,
				uint32 flags)
{
  //Nothing to do.
  return 0;
}

int bdq_io_start(Binlog_relay_IO_param *param)
{
  //Nothing to do.
  return 0;
}

int bdq_io_end(Binlog_relay_IO_param *param)
{
  //Nothing to do.
  return 0;
}

int bdq_sql_stop(Binlog_relay_IO_param *param, bool aborted)
{
  return 0;
}

C_MODE_END

void switch_recycle_bin_status(char in_status)
{
  if(in_status && !recycle_bin_status)
  {
    recycle_bin_status = 1;
    sql_print_information("Plugin recycle_bin switch Recycle_bin_status ON");

  }
  if(!in_status && recycle_bin_status)
  {
    recycle_bin_status = 0;
    sql_print_information("Plugin recycle_bin switch Recycle_bin_status OFF");
  }
  return;
}

static void fix_recycle_bin_enabled(MYSQL_THD thd,
                                    SYS_VAR *var,
                                    void *ptr,
                                    const void *val)
{
  *(char *)ptr= *(char *)val;
  bdq_slave.setRecycleBinEnabled(recycle_bin_enabled != 0);
  if(!recycle_bin_enabled) //set global recycle_bin_enabled=OFF;
  {
    switch_recycle_bin_status(recycle_bin_enabled);
  }
  else  //set global recycle_bin_enabled=ON;
  {
    if(received_fde)
    {
      switch_recycle_bin_status(recycle_bin_enabled);
    }
  }
  return;
}

static void fix_recycle_bin_trace_level(MYSQL_THD thd,
                                        SYS_VAR *var,
                                        void *ptr,
                                        const void *val)
{
  *(unsigned long *)ptr= *(unsigned long *)val;
  bdq_slave.setTraceLevel(recycle_bin_trace_level);
  return;
}

/* plugin system variables */
static MYSQL_SYSVAR_BOOL(enabled, recycle_bin_enabled,
  PLUGIN_VAR_OPCMDARG,
 "Enable recycle_bin plugin(disabled by default). ",
  NULL,				   // check
                         &fix_recycle_bin_enabled, // update
  0);

static MYSQL_SYSVAR_ULONG(trace_level, recycle_bin_trace_level,
  PLUGIN_VAR_OPCMDARG,
 "The tracing level for recycle_bin.",
  NULL,				  // check
                          &fix_recycle_bin_trace_level, // update
  32, 0, ~0UL, 1);

static MYSQL_SYSVAR_ULONG(expire_hours,
        recycle_bin_expire_hours,
        PLUGIN_VAR_OPCMDARG,
        "recycle bin expire hours",
        NULL,
        NULL,
        2,0,48,1);

static SYS_VAR* bdq_system_vars[]= {
  MYSQL_SYSVAR(enabled),
  MYSQL_SYSVAR(trace_level),
  MYSQL_SYSVAR(expire_hours),
  NULL,
};


/* plugin status variables */
static SHOW_VAR bdq_status_vars[]= {
  {"Recycle_bin_status",
   (char*) &recycle_bin_status, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
  {NULL, NULL, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
};

Binlog_relay_IO_observer bdq_relay_io_observer = {
  sizeof(Binlog_relay_IO_observer), // len

  bdq_io_start,	// start
  bdq_io_end,	// stop
  bdq_sql_stop,     // stop sql thread
  bdq_request_dump,	// request_transmit
  bdq_read_event,	// after_read_event
  bdq_queue_event,	// after_queue_event
  bdq_reset_slave,	// reset
};

static int bdq_plugin_init(void *p)
{
//  if (repl_semisync.initObject())
//    return 1;
  if(register_binlog_relay_io_observer(&bdq_relay_io_observer, p))
  {
    return 1;
  }

  glob_description_event= new Format_description_log_event(3);
  return 0;
}

static int bdq_plugin_deinit(void *p)
{
   if (unregister_binlog_relay_io_observer(&bdq_relay_io_observer, p))
   {
     delete glob_description_event;
     return 1;
   }
   delete glob_description_event;
   return 0;
}


struct Mysql_replication recycle_bin= {
  MYSQL_REPLICATION_INTERFACE_VERSION
};

/*
  Plugin library descriptor
*/
mysql_declare_plugin(bdq)
{
  MYSQL_REPLICATION_PLUGIN,
  &recycle_bin,
  "recycle_bin",
  "Ashe Sun",
  "MySQL Recycle Bin",
  PLUGIN_LICENSE_GPL,
  bdq_plugin_init, /* Plugin Init */
  bdq_plugin_deinit, /* Plugin Deinit */
  0x0100 /* 1.0 */,
  bdq_status_vars,	/* status variables */
  bdq_system_vars,	/* system variables */
  NULL,                         /* config options */
  0,                            /* flags */
}
mysql_declare_plugin_end;