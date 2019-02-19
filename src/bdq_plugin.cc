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
#include <sql_handler.h>
#include "log_event.h"
#include "sql_parse.h"
#include "conn_handler/channel_info.h"
#include "sql_rename.h"
#include "sql_db.h"
#include "sql_table.h"
#include "bdq_purger.h"
#include "mysys_err.h"
#include "my_byteorder.h"

#ifdef HAVE_PSI_INTERFACE
PSI_mutex_key  key_ss_mutex_bdq_purge_mutex;
PSI_cond_key key_ss_cond_bdq_purge_cond;
PSI_thread_key key_ss_thread_bdq_purge_thread;
#endif /* HAVE_PSI_INTERFACE */






void switch_recycle_bin_status(char in_status);
bool purged_table();


bdqSlave bdq_slave;
static Format_description_log_event*  glob_description_event = NULL;
static my_bool opt_verify_binlog_checksum= TRUE;
static my_bool received_fde = FALSE;
THD* bdq_backup_thd = NULL;
static char query_create_table[]="CREATE TABLE  (id int not null auto_increment primary key)ENGINE=INNODB";
static const char query_create_backup_database[] = "create database if not exists ``";
const int iso8601_size= 33;
static const char recycle_bin_time_flag[] = "ashesun";

ulonglong make_recycle_bin_iso8601_timestamp(char *buf, ulonglong utime = 0)
{
  struct tm  my_tm;
//  char       tzinfo[7]="Z";  // max 6 chars plus \0
  size_t     len;
  time_t     seconds;

  if (utime == 0)
    utime= my_micro_time();

  seconds= utime / 1000000;
  utime = utime % 1000000;

  if (opt_log_timestamps == 0)
    gmtime_r(&seconds, &my_tm);
  else
  {
    localtime_r(&seconds, &my_tm);

//#ifdef __FreeBSD__
//    /*
//      The field tm_gmtoff is the offset (in seconds) of the time represented
//      from UTC, with positive values indicating east of the Prime Meridian.
//    */
//    long tim= -my_tm.tm_gmtoff;
//#elif _WIN32
//    long tim = _timezone;
//#else
//    long tim= timezone; // seconds West of UTC.
//#endif
//    char dir= '-';
//
//    if (tim < 0)
//    {
//      dir= '+';
//      tim= -tim;
//    }
//    my_snprintf(tzinfo, sizeof(tzinfo), "%c%02d:%02d",
//                dir, (int) (tim / (60 * 60)), (int) ((tim / 60) % 60));
  }

//  len= my_snprintf(buf, iso8601_size, "%04d%02d%02d%02d%02d%02d",
//                   my_tm.tm_year + 1900,
//                   my_tm.tm_mon  + 1,
//                   my_tm.tm_mday,
//                   my_tm.tm_hour,
//                   my_tm.tm_min,
//                   my_tm.tm_sec);
  utime = seconds*1000000+utime;
  len = my_snprintf(buf,iso8601_size,"%lu",utime);

  return utime;
}

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
  char* query_rename_table = new char[strlen("RENAME TABLE %s.%s to %s.%s_%s")+ strlen(recycle_bin_database_name)+
                                      (strlen(db)+strlen(table_dropped_name))*2 + iso8601_size +
                                      strlen(recycle_bin_time_flag)];

  char* query_create_virtual_table = new char[strlen(query_create_table) + strlen(db) + strlen(table_dropped_name)];

  char* query_create_recycle_bin_db = new char[strlen(query_create_backup_database)+strlen(recycle_bin_database_name)];
  int res = FALSE;
  my_bool backup_complete = FALSE;
  char my_timestamp[iso8601_size];

  make_recycle_bin_iso8601_timestamp(my_timestamp);

  //stage 1.改变当前线程的sql_log_bin = 0;不写binlog操作。
  if (bdq_backup_thd->variables.sql_log_bin)
    bdq_backup_thd->variables.option_bits |= OPTION_BIN_LOG;
  else
    bdq_backup_thd->variables.option_bits &= ~OPTION_BIN_LOG;

  //stage 2.create database if not exists.
  sprintf(query_create_recycle_bin_db,"create database if not exists %s",recycle_bin_database_name);
  if(bdq_prepare_execute_command(bdq_backup_thd,
                                 query_create_recycle_bin_db,db,table_dropped_name))
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
      backup_complete = FALSE;
      goto exit_bdq_btr;
    }
  }
  else
  {
    backup_complete = FALSE;
    goto exit_bdq_btr;
  }

  //stage 3.rename A.a for backup.
  sprintf(query_rename_table,"RENAME TABLE `%s`.`%s` to `%s`.`%s_%s_%s_%s`",
          db,table_dropped_name,
          recycle_bin_database_name,db,table_dropped_name,recycle_bin_time_flag,my_timestamp);

  if((strlen(db)+strlen(table_dropped_name)+strlen(recycle_bin_time_flag)+strlen(my_timestamp)) > 64)
  {
    backup_complete = FALSE;
    sql_print_error("Backup table name is too long,%s",query_rename_table);
    goto exit_bdq_btr;
  }
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
      backup_complete = FALSE;
      goto exit_bdq_btr;
    }
    bdq_after_execute_command(bdq_backup_thd);
  }
  else
  {
    backup_complete = FALSE;
    goto exit_bdq_btr;
  }

  //在数据库层执行完rename之后就是已经完成备份操作了，但是如果stage 4出错的话，可能会影响复制的SQL线程正常回放。
  backup_complete = TRUE;
  //stage 4.create A.a(id int not null auto_increment primary key);
  sprintf(query_create_virtual_table,
          "CREATE TABLE %s.%s(id int not null auto_increment primary key)ENGINE=INNODB",db,table_dropped_name);
  if(bdq_prepare_execute_command(bdq_backup_thd,query_create_virtual_table,db,table_dropped_name))
  {
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
  delete[] query_create_recycle_bin_db;

  return backup_complete;
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

  if(type == binary_log::HEARTBEAT_LOG_EVENT)
  {
    purged_table();
  }

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
      return 0;
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
  bdq_backup_thd = (THD*)my_get_thread_local(THR_THD);
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

static MYSQL_SYSVAR_ULONG(expire_seconds,
        recycle_bin_expire_seconds,
        PLUGIN_VAR_OPCMDARG,
        "recycle bin expire hours",
        NULL,
        NULL,
        1800,0,172800,1);

static MYSQL_SYSVAR_STR(database_name,
        recycle_bin_database_name,
                        PLUGIN_VAR_OPCMDARG,
                        "recycle bin database name",
                        NULL,
                        NULL,
                        "recycle_bin_dba"
        );

static SYS_VAR* bdq_system_vars[]= {
  MYSQL_SYSVAR(enabled),
  MYSQL_SYSVAR(trace_level),
  MYSQL_SYSVAR(expire_seconds),
  MYSQL_SYSVAR(database_name),
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


long mysql_rm_arc_files(THD *thd, MY_DIR *dirp, const char *org_path);

const char *del_exts[]= {".frm", ".BAK", ".TMD", ".opt", ".OLD", ".cfg", NullS};
static TYPELIB deletable_extentions=
        {array_elements(del_exts)-1,"del_exts", del_exts, NULL};

static bool find_db_tables_should_be_purged(THD *thd, MY_DIR *dirp,
                                              const char *db,
                                              const char *path,
                                              TABLE_LIST **tables,
                                              bool *found_other_files,ulonglong utime)
{
  char filePath[FN_REFLEN];
  TABLE_LIST *tot_list=0, **tot_list_next_local, **tot_list_next_global;
  DBUG_ENTER("find_db_tables_and_rm_known_files");
  DBUG_PRINT("enter",("path: %s", path));
  TYPELIB *known_extensions= ha_known_exts();
  ulonglong table_backup_time = 0;

  tot_list_next_local= tot_list_next_global= &tot_list;

  for (uint idx=0 ;
       idx < dirp->number_off_files && !thd->killed ;
       idx++)
  {
    table_backup_time = 0;
    FILEINFO *file=dirp->dir_entry+idx;
    char *extension;
    DBUG_PRINT("info",("Examining: %s", file->name));

    /* skiping . and .. */
    if (file->name[0] == '.' && (!file->name[1] ||
                                 (file->name[1] == '.' &&  !file->name[2])))
      continue;

    if (file->name[0] == 'a' && file->name[1] == 'r' &&
        file->name[2] == 'c' && file->name[3] == '\0')
    {
      /* .frm archive:
        Those archives are obsolete, but following code should
        exist to remove existent "arc" directories.
      */
      char newpath[FN_REFLEN];
      MY_DIR *new_dirp;
      strxmov(newpath, path, "/", "arc", NullS);
      (void) unpack_filename(newpath, newpath);
      if ((new_dirp = my_dir(newpath, MYF(MY_DONT_SORT))))
      {
        DBUG_PRINT("my",("Archive subdir found: %s", newpath));
        if ((mysql_rm_arc_files(thd, new_dirp, newpath)) < 0)
          DBUG_RETURN(true);
        continue;
      }
      *found_other_files= true;
      continue;
    }
    if (!(extension= strrchr(file->name, '.')))
      extension= strend(file->name);
    if (find_type(extension, &deletable_extentions, FIND_TYPE_NO_PREFIX) <= 0)
    {
      if (find_type(extension, known_extensions, FIND_TYPE_NO_PREFIX) <= 0)
        *found_other_files= true;
      continue;
    }
    /* just for safety we use files_charset_info */
    if (db && !my_strcasecmp(files_charset_info,
                             extension, reg_ext))
    {
      /* Drop the table nicely */
      *extension= 0;			// Remove extension
      TABLE_LIST *table_list=(TABLE_LIST*)
              thd->mem_calloc(sizeof(*table_list) +
                              strlen(db) + 1 +
                              MYSQL50_TABLE_NAME_PREFIX_LENGTH +
                              strlen(file->name) + 1);

      if (!table_list)
        DBUG_RETURN(true);
      table_list->db= (char*) (table_list+1);
      table_list->db_length= my_stpcpy(const_cast<char*>(table_list->db),
                                       db) - table_list->db;
      table_list->table_name= table_list->db + table_list->db_length + 1;
      table_list->table_name_length= filename_to_tablename(file->name,
                                                           const_cast<char*>(table_list->table_name),
                                                           MYSQL50_TABLE_NAME_PREFIX_LENGTH +
                                                           strlen(file->name) + 1);
      table_list->open_type= OT_BASE_ONLY;

      /* To be able to correctly look up the table in the table cache. */
      if (lower_case_table_names)
        table_list->table_name_length= my_casedn_str(files_charset_info,
                                                     const_cast<char*>(table_list->table_name));

      table_list->alias= table_list->table_name;	// If lower_case_table_names=2
      table_list->internal_tmp_table= is_prefix(file->name, tmp_file_prefix);
      MDL_REQUEST_INIT(&table_list->mdl_request,
                       MDL_key::TABLE, table_list->db,
                       table_list->table_name, MDL_EXCLUSIVE,
                       MDL_TRANSACTION);

      int time_buf_start_pos =0;
      for(;time_buf_start_pos < table_list->table_name_length;time_buf_start_pos++)
      {
        if(strncasecmp(table_list->table_name+time_buf_start_pos,
                recycle_bin_time_flag,sizeof(recycle_bin_time_flag)-1) ==0 )
        {
          time_buf_start_pos +=strlen(recycle_bin_time_flag);
          break;
        }
      }

      if(time_buf_start_pos == table_list->table_name_length)
      {
        continue;
      }

      //int8store((char*)(table_list->table_name+time_buf_start_pos+1),table_backup_time);
      table_backup_time = atoll(table_list->table_name + time_buf_start_pos+1);

      if(table_backup_time > utime) //未到purge 时间
      {
        continue;
      }
      /* Link into list */
      (*tot_list_next_local)= table_list;
      (*tot_list_next_global)= table_list;
      tot_list_next_local= &table_list->next_local;
      tot_list_next_global= &table_list->next_global;
    }
    else
    {
//      strxmov(filePath, path, "/", file->name, NullS);
//      /*
//        We ignore ENOENT error in order to skip files that was deleted
//        by concurrently running statement like REAPIR TABLE ...
//      */
//      if (my_delete_with_symlink(filePath, MYF(0)) &&
//          my_errno() != ENOENT)
//      {
//        char errbuf[MYSYS_STRERROR_SIZE];
//        my_error(EE_DELETE, MYF(0), filePath,
//                 my_errno(), my_strerror(errbuf, sizeof(errbuf), my_errno()));
//        DBUG_RETURN(true);
//      }
    }
  }
  *tables= tot_list;
  DBUG_RETURN(false);
}

bool purge_tables_before_time(THD* thd,st_mysql_const_lex_string db,bool if_exists,bool silent,ulonglong utime)
{
  Drop_table_error_handler err_handler;
  ulong deleted_tables= 0;
  bool error= true;
  char	path[2 * FN_REFLEN + 16];
  MY_DIR *dirp;
  size_t length;
  bool found_other_files= false;
  TABLE_LIST *tables= NULL;
  TABLE_LIST *table;
  //Drop_table_error_handler err_handler;
  DBUG_ENTER("mysql_rm_db");


  if (lock_schema_name(thd, db.str))
    DBUG_RETURN(true);

  length= build_table_filename(path, sizeof(path) - 1, db.str, "", "", 0);
  my_stpcpy(path+length, MY_DB_OPT_FILE);		// Append db option file name
  // del_dbopt(path);				// Remove dboption hash entry
  path[length]= '\0';				// Remove file name

  /* See if the directory exists */
  if (!(dirp= my_dir(path,MYF(MY_DONT_SORT))))
  {
    if (!if_exists)
    {
      my_error(ER_DB_DROP_EXISTS, MYF(0), db.str);
      DBUG_RETURN(true);
    }
    else
    {
      push_warning_printf(thd, Sql_condition::SL_NOTE,
                          ER_DB_DROP_EXISTS, ER(ER_DB_DROP_EXISTS), db.str);
      error= false;
    }
  }

  if ((error = find_db_tables_should_be_purged(thd, dirp, db.str, path, &tables,
                                        &found_other_files,utime)) )
  {
    goto exit;
  }

  if(!tables)
  {
    goto exit;
  }

  if ((error=lock_table_names(thd, tables, NULL, thd->variables.lock_wait_timeout, 0)))
  {
    goto exit;
  }

  if(tables)
  {
    mysql_ha_rm_tables(thd, tables);
  }

  for (table= tables; table; table= table->next_local)
  {
    tdc_remove_table(thd, TDC_RT_REMOVE_ALL, table->db, table->table_name,
                     false);
    deleted_tables++;
  }
  thd->push_internal_handler(&err_handler);
  if (!thd->killed &&
      !(tables &&
        mysql_rm_table_no_locks(thd, tables, true, false, true, true)))
  {
    //nothing to do.
  }
  thd->pop_internal_handler();

  exit:
  /*
    If this database was the client's selected database, we silently
    change the client's selected database to nothing (to have an empty
    SELECT DATABASE() in the future). For this we free() thd->db and set
    it to 0.
  */

 // bdq_after_execute_command(thd);

  bdq_backup_thd->mdl_context.release_statement_locks();
  bdq_backup_thd->mdl_context.release_transactional_locks();

  DBUG_RETURN(error);
}

bool purged_table()
{
  char now_timestamp[iso8601_size];
  ulonglong now_utime = make_recycle_bin_iso8601_timestamp(now_timestamp);
  ulonglong table_should_be_purged_time = now_utime - recycle_bin_expire_seconds*1000000;
  st_mysql_const_lex_string db_string;
  db_string.str = recycle_bin_database_name;
  db_string.length = strlen(recycle_bin_database_name);

  if(purge_tables_before_time(bdq_backup_thd,db_string,true,true,table_should_be_purged_time))
  {
    sql_print_error("purged tables failed");
  }
}