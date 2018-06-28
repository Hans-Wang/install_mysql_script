#!/usr/bin/env python
# -*- coding:utf-8 -*-
# by:Hans Wong
# mail:hans@pymysql.com

import os
import re
import yum
import sys
import pwd
import grp
import tarfile
import subprocess



def exec_cmd(cmd):
    CMD = subprocess.Popen(cmd,
                     shell=True,
                     stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
    stdout, stderr = CMD.communicate()

    if CMD.returncode != 0:
        return CMD.returncode, stderr
    return CMD.returncode, stdout

def yum_install(args):
    yb=yum.YumBase()
    #inst = yb.rpmdb.returnPackages()
    #installed=[x.name for x in inst]
    args = args.strip()
    packages=[]
    packages.append(args)

    for package in packages:
        print('Installing {0}'.format(package))
        kwarg = {
            'name':package
        }
        yb.install(**kwarg)
        yb.resolveDeps()
        yb.buildTransaction()
        yb.processTransaction()


def create_group(groupname):
    if not g_info:
        create_group = "groupadd %s" %(groupname)
        returncode, out = exec_cmd(create_group)
        if returncode == 0:
            print("create {} group is OK".format(groupname))
        else:
            print(out)
def create_user(username):
    if not u_info:
        create_user = "adduser %s -g %s -s /sbin/nologin -d /usr/local/mysql -MN " %(username, groupname) 
        returncode, out = exec_cmd(create_user)
        if returncode == 0:
            print("create {} user is OK".format(username))
        else:
            print(out)


def create_dir(install_dir,data_dir,logs,tmp,data):

    if os.path.exists(install_dir):
        print("{} directory is already exists. ".format(install_dir))
    else:
        os.makedirs(install_dir)

    if os.path.exists(data_dir):
        print("{} directory is already exists. ".format(data_dir))
    else:
        os.makedirs(data_dir)
        os.chdir(data_dir)
        for i in (logs, tmp, data):
            if not os.path.exists(i):
                os.makedirs(i)
    exec_cmd('chown -R %s:%s %s' % (username,groupname, data_dir))

def unpacke(package,links,mysql_install_dir):
    package_dir = os.path.splitext(os.path.splitext(package)[0])[0].split('/')[-1]
    print(package_dir)
    if not os.path.exists(mysql_install_dir+ "/" + package_dir):
        with tarfile.open(package,'r:gz') as tar:
            tar.extractall(mysql_install_dir)
    else:
        print("Install package already uncompression.")

   
    try:
        os.symlink(mysql_install_dir+'/'+package_dir, links)
    except OSError:
        exec_cmd('chown -R %s:%s %s' % (username,groupname, mysql_install_dir))
        exec_cmd('chown -R %s:%s %s' % (username,groupname, links))
        check_mysqld(links)
        return '%s File exists' % links
    exec_cmd('chown -R %s:%s %s' % (username,groupname, mysql_install_dir))
    exec_cmd('chown -R %s:%s %s' % (username,groupname, links))
    check_mysqld(links)


def check_mysqld(links):
    code,out = exec_cmd("/usr/bin/ldd {0}/bin/mysqld".format(links))

    uninstall_packge = re.findall('not found', out)

    if uninstall_packge:
        for file_modul in out.split('\n'):
            if "not found" in file_modul :
                packe = file_modul.split('.')[0]
                yum_install(packe)

    
def initialize_mysql(mysql_cmd, mysql_file, mysql_option=''):
    config = """
[client]
port            = {0} 

[mysql]
auto-rehash
prompt="\\u@\\h [\\d]>"
#pager="less -i -n -S"
#tee=/opt/mysql/query.log

[mysqld]
####: for global
user                                =mysql                          #   mysql
basedir                             =/usr/local/mysql/              #   /usr/local/mysql/
datadir                             =/data/mysql/mysql{0}/data     #     /usr/local/mysql/data
server_id                           =100{0}                        #   0
port                                ={0}                           #   3306
character_set_server                =utf8                           #   latin1
explicit_defaults_for_timestamp     =off                            #    off
log_timestamps                      =system                         #   utc
socket                              =/tmp/mysql{0}.sock                #   /tmp/mysql.sock
read_only                           =0                              #   off
skip_name_resolve                   =1                              #   0
auto_increment_increment            =1                              #   1
auto_increment_offset               =1                              #   1
lower_case_table_names              =1                              #   0
secure_file_priv                    =                               #   null
open_files_limit                    =65536                          #   1024
max_connections                     =1000                           #   151
thread_cache_size                   =64                             #   9
table_open_cache                    =81920                          #   2000
table_definition_cache              =4096                           #   1400
table_open_cache_instances          =64                             #   16
max_prepared_stmt_count             =1048576                        #

####: for binlog
binlog_format                       =row                          #     row
log_bin                             =mysql-bin                      #   off
binlog_rows_query_log_events        =on                             #   off
log_slave_updates                   =on                             #   off
expire_logs_days                    =7                              #   0
binlog_cache_size                   =65536                          #   65536(64k)
#binlog_checksum                     =none                           #  CRC32
sync_binlog                         =1                              #   1
slave-preserve-commit-order         =ON                             #

####: for error-log
log_error                           =error.log                        # /usr/local/mysql/data/localhost.localdomain.err

general_log                         =off                            #   off
general_log_file                    =general.log                    #   hostname.log

####: for slow query log
slow_query_log                      =on                             #    off
slow_query_log_file                 =slow.log                       #    hostname.log
#log_queries_not_using_indexes       =on                             #    off
long_query_time                     =1.000000                       #    10.000000

####: for gtid
#gtid_executed_compression_period    =1000                          #   1000
gtid_mode                           =on                            #    off
enforce_gtid_consistency            =on                            #    off


####: for replication
skip_slave_start                     =1                              #
#master_info_repository              =table                         #   file
#relay_log_info_repository           =table                         #   file
slave_parallel_type                  =logical_clock                 #    database | LOGICAL_CLOCK
slave_parallel_workers               =4                             #    0
#rpl_semi_sync_master_enabled        =1                             #    0
#rpl_semi_sync_slave_enabled         =1                             #    0
#rpl_semi_sync_master_timeout        =1000                          #    1000(1 second)
#plugin_load_add                     =semisync_master.so            #
#plugin_load_add                     =semisync_slave.so             #
binlog_group_commit_sync_delay       =100                           #    500(0.05%秒)、默认值0
binlog_group_commit_sync_no_delay_count = 10                       #    0


####: for innodb
default_storage_engine                          =innodb                     #   innodb
default_tmp_storage_engine                      =innodb                     #   innodb
innodb_data_file_path                           =ibdata1:100M:autoextend    #   ibdata1:12M:autoextend
innodb_temp_data_file_path                      =ibtmp1:12M:autoextend      #   ibtmp1:12M:autoextend
innodb_buffer_pool_filename                     =ib_buffer_pool             #   ib_buffer_pool
innodb_log_group_home_dir                       =./                         #   ./
innodb_log_files_in_group                       =3                          #   2
innodb_log_file_size                            =100M                       #   50331648(48M)
innodb_file_per_table                           =on                         #   on
innodb_online_alter_log_max_size                =128M                       #   134217728(128M)
innodb_open_files                               =65535                      #   2000
innodb_page_size                                =16k                        #   16384(16k)
innodb_thread_concurrency                       =0                          #   0
innodb_read_io_threads                          =4                          #   4
innodb_write_io_threads                         =4                          #   4
innodb_purge_threads                            =4                          #   4(垃圾回收)
innodb_page_cleaners                            =4                          #   4(刷新lru脏页)
innodb_print_all_deadlocks                      =on                         #   off
innodb_deadlock_detect                          =on                         #   on
innodb_lock_wait_timeout                        =20                         #   50
innodb_spin_wait_delay                          =128                          # 6
innodb_autoinc_lock_mode                        =2                          #   1
innodb_io_capacity                              =200                        #   200
innodb_io_capacity_max                          =2000                       #   2000
#--------Persistent Optimizer Statistics
innodb_stats_auto_recalc                        =on                         #   on
innodb_stats_persistent                         =on                         #   on
innodb_stats_persistent_sample_pages            =20                         #   20

innodb_adaptive_hash_index                      =on                         #   on
innodb_change_buffering                         =all                        #   all
innodb_change_buffer_max_size                   =25                         #   25
innodb_flush_neighbors                          =1                          #   1
#innodb_flush_method                             =                           #
innodb_doublewrite                              =on                         #   on
innodb_log_buffer_size                          =128M                        #  16777216(16M)
innodb_flush_log_at_timeout                     =1                          #   1
innodb_flush_log_at_trx_commit                  =1                          #   1
innodb_buffer_pool_size                         =100M                  #        134217728(128M)
innodb_buffer_pool_instances                    =4
autocommit                                      =1                          #   1
#--------innodb scan resistant
innodb_old_blocks_pct                           =37                         #    37
innodb_old_blocks_time                          =1000                       #    1000
#--------innodb read ahead
innodb_read_ahead_threshold                     =56                         #    56 (0..64)
innodb_random_read_ahead                        =OFF                        #    OFF
#--------innodb buffer pool state
innodb_buffer_pool_dump_pct                     =25                         #    25
innodb_buffer_pool_dump_at_shutdown             =ON                         #    ON
innodb_buffer_pool_load_at_startup              =ON                         #    ON




####  for performance_schema
performance_schema                                                      =off   #    on
performance_schema_consumer_global_instrumentation                      =on    #    on
performance_schema_consumer_thread_instrumentation                      =on    #    on
performance_schema_consumer_events_stages_current                       =on    #    off
performance_schema_consumer_events_stages_history                       =on    #    off
performance_schema_consumer_events_stages_history_long                  =off   #    off
performance_schema_consumer_statements_digest                           =on    #    on
performance_schema_consumer_events_statements_current                   =on    #    on
performance_schema_consumer_events_statements_history                   =on    #    on
performance_schema_consumer_events_statements_history_long              =on    #    off
performance_schema_consumer_events_waits_current                        =on    #    off
performance_schema_consumer_events_waits_history                        =on    #    off
performance_schema_consumer_events_waits_history_long                   =off   #    off
performance-schema-instrument                                           ='memory/%=COUNTED'
""".format(port)
    
    with open(mysql_file, 'w') as f:
        f.write(config)
    
    cmd = '%s --defaults-file=%s %s'  % (mysql_cmd, mysql_file, mysql_option)
    returncode, out = exec_cmd(cmd)
    if returncode != 0:
        raise SystemExit('execut %s error:%s'%(cmd, out ))
    else:
        print('Execute %s successful!'%(cmd))


def get_packenanme(curre_path):

    filename_list = os.listdir(curre_path)
    for filename in filename_list:
        if 'mysql' in filename and 'tar.gz' in filename:
            return filename 
        



def main():
    
    try:
        g_info = grp.getgrnam(groupname)
        print("{} group is already exists".format(groupname))
    except KeyError as e:
        print("{0} group is not found,start create group {0}".format(groupname))
        create_group(groupname)
    
    try:
        u_info = pwd.getpwnam(username)
        print("{} user is already exists".format(username))
    except KeyError as e:
        print("{0} user is not found,start create user {0}".format(username))
        create_user(username)

    create_dir(mysql_install_dir,mysql_data_dir,'logs','tmp','data')
    unpacke(package,links,mysql_install_dir)
    initialize_mysql('%s/bin/mysqld' % links, mysql_file, '--initialize-insecure') 


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        raise SystemExit('Using: %s port  '%(sys.argv[0] ))
    
    port = sys.argv[1]
    username = 'mysql'
    groupname = username
    mysql_install_dir = '/opt/mysql'
    mysql_data_dir = '/data/mysql/mysql{}'.format(port)
    #package = os.path.abspath("mysql-5.7.22-linux-glibc2.12-x86_64.tar.gz")
    curre_path = os.getcwd()
    print(curre_path )
    package = get_packenanme(curre_path)
    print(package)
    links = '/usr/local/mysql'
    mysql_file = '{0}/my{1}.cnf'.format(mysql_data_dir, port)
    g_info = None
    u_info = None
    main()
