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
#include "log_event.h"
#include "sql_parse.h"

bdqSlave bdq_slave;
static Format_description_log_event*  glob_description_event = NULL;
static my_bool opt_verify_binlog_checksum= TRUE;
static my_bool received_fde = FALSE;
static THD* bdq_backup_thd = new THD;

/*
  indicate whether or not the slave should send a reply to the master.

  This is set to true in repl_semi_slave_read_event if the current
  event read is the last event of a transaction. And the value is
  checked in repl_semi_slave_queue_event.
*/
bool semi_sync_need_reply= false;

C_MODE_START

int bdq_reset_slave(Binlog_relay_IO_param *param)
{
  // TODO: reset semi-sync slave status here
  return 0;
}

int bdq_request_dump(Binlog_relay_IO_param *param,
				 uint32 flags)
{
  return 0;
}

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
  mysql_parse(bdq_backup_thd, &parser_state);


  return TRUE;
}

int bdq_read_event(Binlog_relay_IO_param *param,
			       const char *packet, unsigned long len,
			       const char **event_buf, unsigned long *event_len)
{
  Log_event *ev= NULL;
  Log_event_type type= binary_log::UNKNOWN_EVENT;
  my_bool need_backup = FALSE;
  const char* tmp_event_buf;
  unsigned long tmp_event_len;

  const char *error_msg= NULL;

  int semisync = bdq_slave.semisync_event(packet, len,
                                          &semi_sync_need_reply,
                                          &tmp_event_buf, &tmp_event_len);
  if(semisync)
  {
    type=(Log_event_type)packet[EVENT_TYPE_OFFSET+2];
  }
  else
  {
    type=(Log_event_type)packet[EVENT_TYPE_OFFSET];
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
        sql_print_error("Could not construct log event object: %s", error_msg);
      }
      delete glob_description_event;
      glob_description_event= (Format_description_log_event*) ev;
      received_fde = TRUE;
    }
    else
    {
      return 0;
    }
  }

  //已经收到了FORMAT_DESCRIPTION_EVENT，再对event type进行判断。
  if(type == binary_log::QUERY_EVENT)
  {
    //check need backup(drop table) @todo drop schema backup.
//    Log_event* ev = Log_event::read_log_event(file, glob_description_event,
//                                              opt_verify_binlog_checksum,
//                                              rewrite_db_filter);



//    ev  = new Query_log_event(tmp_event_buf, tmp_event_len,(const Format_description_log_event* )glob_description_event,
//                              binary_log::QUERY_EVENT);

    //glob_description_event->common_footer->checksum_alg = binary_log::BINLOG_CHECKSUM_ALG_CRC32;
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

    if(strlen(qe->query))
    {
      sql_print_information("query event->query:%s",qe->query);
    }

    if(bdq_backup(qe->query,qe->db,home_dir));
  }
  else
  {
    return 0;
  }


  return 0;
}

int bdq_queue_event(Binlog_relay_IO_param *param,
				const char *event_buf,
				unsigned long event_len,
				uint32 flags)
{
//  if (rpl_semi_sync_slave_status && semi_sync_need_reply)
//  {
//    /*
//      We deliberately ignore the error in slaveReply, such error
//      should not cause the slave IO thread to stop, and the error
//      messages are already reported.
//    */
//    (void) repl_semisync.slaveReply(param->mysql,
//                                    param->master_log_name,
//                                    param->master_log_pos);
//  }
  return 0;
}

int bdq_io_start(Binlog_relay_IO_param *param)
{
  //return repl_semisync.slaveStart(param);
  return 0;
}

int bdq_io_end(Binlog_relay_IO_param *param)
{
  //return repl_semisync.slaveStop(param);
  return 0;
}

int bdq_sql_stop(Binlog_relay_IO_param *param, bool aborted)
{
  return 0;
}

C_MODE_END

static void fix_rpl_semi_sync_slave_enabled(MYSQL_THD thd,
					    SYS_VAR *var,
					    void *ptr,
					    const void *val)
{
  *(char *)ptr= *(char *)val;
  bdq_slave.setSlaveEnabled(bdq_slave_enabled != 0);
  return;
}

static void fix_rpl_semi_sync_trace_level(MYSQL_THD thd,
					  SYS_VAR *var,
					  void *ptr,
					  const void *val)
{
  *(unsigned long *)ptr= *(unsigned long *)val;
  bdq_slave.setTraceLevel(rpl_semi_sync_slave_trace_level);
  return;
}

/* plugin system variables */
static MYSQL_SYSVAR_BOOL(enabled, bdq_slave_enabled,
  PLUGIN_VAR_OPCMDARG,
 "Enable semi-synchronous replication slave (disabled by default). ",
  NULL,				   // check
  &fix_rpl_semi_sync_slave_enabled, // update
  0);

static MYSQL_SYSVAR_ULONG(trace_level, rpl_semi_sync_slave_trace_level,
  PLUGIN_VAR_OPCMDARG,
 "The tracing level for semi-sync replication.",
  NULL,				  // check
  &fix_rpl_semi_sync_trace_level, // update
  32, 0, ~0UL, 1);

static SYS_VAR* bdq_system_vars[]= {
  MYSQL_SYSVAR(enabled),
  MYSQL_SYSVAR(trace_level),
  NULL,
};


/* plugin status variables */
static SHOW_VAR bdq_status_vars[]= {
  {"Rpl_semi_sync_slave_status",
   (char*) &rpl_semi_sync_slave_status, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
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
    return 1;

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


struct Mysql_replication bdq_plugin= {
  MYSQL_REPLICATION_INTERFACE_VERSION
};

/*
  Plugin library descriptor
*/
mysql_declare_plugin(bdq)
{
  MYSQL_REPLICATION_PLUGIN,
  &bdq_plugin,
  "bdq",
  "Ashe Sun",
  "backup drop query",
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
