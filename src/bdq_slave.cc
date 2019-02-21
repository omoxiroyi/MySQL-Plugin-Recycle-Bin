/* Copyright (c) 2008, 2015, Oracle and/or its affiliates. All rights reserved.

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
#include "mysql.h"
#include "debug_sync.h"

char recycle_bin_enabled;
unsigned long recycle_bin_trace_level;
unsigned long recycle_bin_expire_seconds;
char* recycle_bin_database_name;
unsigned long recycle_bin_check_sql_delay_period;



char recycle_bin_status= 0;
ulonglong recycle_bin_backup_counter = 0;

int bdqSlave::initObject()
{
  int result= 0;
  const char *kWho = "ReplSemiSyncSlave::initObject";

  if (init_done_)
  {
    sql_print_warning("%s called twice", kWho);
    return 1;
  }
  init_done_ = true;

  /* References to the parameter works after set_options(). */
  setRecycleBinEnabled(recycle_bin_enabled);
  setTraceLevel(recycle_bin_trace_level);

  return result;
}

/**
 *
 * @param header
 * @param total_len
 * @param payload
 * @param payload_len
 * @return
 */
int bdqSlave::semisync_event(const char *header,
                             unsigned long total_len,
                             const char **payload,
                             unsigned long *payload_len)
{
  if ((unsigned char)(header[0]) == kPacketMagicNum) //半同步复制环境
  {
    *payload_len = total_len - 2;
    *payload     = header + 2;
    return 1;
  }
  else //异步复制环境
  {
    *payload_len = total_len;
    *payload     = header;
    return 0;
  }
}

int bdqSlave::slaveStart(Binlog_relay_IO_param *param)
{
  bool semi_sync= getSlaveEnabled();
  if (semi_sync && !recycle_bin_status)
    recycle_bin_status= 1;
  return 0;
}


int bdqSlave::slaveStop(Binlog_relay_IO_param *param)
{
  if (recycle_bin_status)
    recycle_bin_status= 0;
  if (mysql_reply)
    mysql_close(mysql_reply);
  mysql_reply= 0;
  return 0;
}

