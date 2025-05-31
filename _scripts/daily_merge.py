#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import argparse
import functools
import concurrent.futures
import datetime
import shutil
import subprocess
import shlex

import code
import pprint

from datetime import datetime, timezone
from pathlib import Path

# Script version
VERSION = '1.1'

# Options definition
parser = argparse.ArgumentParser(description="version: " + VERSION)

input_group = parser.add_argument_group('Input parameters')
input_group.add_argument('-f', '--fresh-dir', help='Input directory of the fresh data to merge', default = None, required = True)
input_group.add_argument('-r', '--referential-dir', help='Referential directory', default = None, required = True)

def print_horizontal_bar(width='large'):
    if width == 'large':
        print('-'*80)
    else:
        print('-'*10)

def print_result(res):
    for i in res:
        print(i)
    print()

def exec_cmd(cmd):
    #cmd = 'ls /var/log | head -n+1'

    # yes, I know, 'shell=True' is very bad
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    print("[+] command:\t%s" % p.args)
    print("[+] retcode:\t%s" % p.returncode)
    
    print("[+] stdout:")
    print_result(p.stdout.splitlines()) if p.stdout.splitlines() else None
    
    print("[+] stderr:")
    print_result(p.stderr.splitlines()) if p.stderr.splitlines() else None
    
    print_horizontal_bar(width='small')
    
    return p

def merge(options):
    fresh = os.path.abspath(options.fresh_dir)
    referential = os.path.abspath(options.referential_dir)
    
    print("[+] fresh dir:\t\t'%s'" % fresh)
    print("[+] referential dir:\t'%s'\n" % referential)
    print_horizontal_bar()
    
    # 1. mv today* files to yesterday*
    for ref_zone_dir, ref_zone_subdirs, ref_zone_files, in sorted(os.walk(referential)):
        # the following if is to ignore self/top dir
        if ref_zone_files:
            print('[+] ref zone dir: "%s"' % ref_zone_dir)
            zone_tld_dest_ref_file_full_path_today = os.path.join(ref_zone_dir, 'today.gz')
            zone_tld_dest_ref_file_full_path_today_new = os.path.join(ref_zone_dir, 'today_new.gz')
            zone_tld_dest_ref_file_full_path_yesterday = os.path.join(ref_zone_dir, 'yesterday.gz')
            zone_tld_dest_ref_file_full_path_yesterday_new = os.path.join(ref_zone_dir, 'yesterday_new.gz')
            #pprint.pprint([zone_tld_dest_ref_file_full_path_today, zone_tld_dest_ref_file_full_path_today_new, zone_tld_dest_ref_file_full_path_yesterday, zone_tld_dest_ref_file_full_path_yesterday_new])

            # 1.1 move ref today to ref yesterday
            print('[+] moving today.gz file to yesterday.gz')
            cmd_today = "cp -f '%s' '%s'" % (zone_tld_dest_ref_file_full_path_today, zone_tld_dest_ref_file_full_path_yesterday)
            exec_cp_ref_today_to_ref_yesterday = exec_cmd(cmd_today)
            if exec_cp_ref_today_to_ref_yesterday.returncode != 0:
                print('[!] something went wrong while moving "%s" to "%s"' % (zone_tld_dest_ref_file_full_path_today, zone_tld_dest_ref_file_full_path_yesterday))
            
            # 1.2. move ref today_new to ref yesterday_new
            print('[+] moving today_new.gz file to yesterday_new.gz')
            cmd_today_new = "cp -f '%s' '%s'" % (zone_tld_dest_ref_file_full_path_today_new, zone_tld_dest_ref_file_full_path_yesterday_new)
            exec_mv_ref_today_new_to_ref_yesterday_new = exec_cmd(cmd_today_new)
            if exec_mv_ref_today_new_to_ref_yesterday_new.returncode != 0:
                print('[!] something went wrong while moving "%s" to "%s"' % (zone_tld_dest_ref_file_full_path_today_new, zone_tld_dest_ref_file_full_path_yesterday_new))
        
            print_horizontal_bar()

    # 2. process fresh files
    for fresh_dir in sorted(os.listdir(fresh)):
        print("[+] fresh directory:\t%s" % fresh_dir)
        full_domain = fresh_dir
        full_domain_rev_split = full_domain[::-1].split('.')
        
        # tld case
        if len(full_domain_rev_split) == 1:
            zone_tld_dest = zone_raw = fresh_dir
         
        # sld case
        elif len(full_domain_rev_split) > 1:
            zone_tld_dest = full_domain_rev_split[0][::-1]
            zone_raw = fresh_dir
        print("[+] fresh zone tld dst:\t%s" % zone_tld_dest)

        zone_raw_fresh_dir_full_path = os.path.join(fresh, zone_raw)
        zone_tld_dest_fresh_dir_full_path = os.path.join(fresh, zone_tld_dest)
        zone_tld_dest_ref_dir_full_path = os.path.join(referential, zone_tld_dest)

        if not(os.path.exists(zone_tld_dest_ref_dir_full_path)):
            print("[!] this tld zone dir exists in fresh but not in ref:\t'%s'" % (zone_tld_dest))
            Path(zone_tld_dest_ref_dir_full_path).mkdir(parents=True, exist_ok=True)
            print("[!] this subfolder has been created in ref:\t\t'%s'" % (zone_tld_dest_ref_dir_full_path))
        
        for fresh_file in sorted(os.listdir(zone_raw_fresh_dir_full_path)):
            print("fresh file: %s" % fresh_file)
            if fresh_file == 'today.gz':
                fresh_file_full_path_today = os.path.join(zone_raw_fresh_dir_full_path, fresh_file)
                ref_file_full_path_today = os.path.join(zone_tld_dest_ref_dir_full_path, fresh_file)
                
                cmd_cat_today = "cat '%s' >> '%s'" % (fresh_file_full_path_today, ref_file_full_path_today)
                #cmd_cat_today = 'bash -c "sort -u <(zcat \'%s\') <(zcat \'%s\') | sponge | gzip > \'%s\'"' % (fresh_file_full_path_today, ref_file_full_path_today, ref_file_full_path_today)
                #cmd_cat_today = "zcat '%s' '%s' | sort -u | sponge | gzip -9 > '%s'" % (fresh_file_full_path_today, ref_file_full_path_today, ref_file_full_path_today)
                exec_append_today_fresh_to_ref_zone = exec_cmd(cmd_cat_today)
                if exec_append_today_fresh_to_ref_zone.returncode != 0:
                    print('[!] something went wrong while appending "%s" to "%s"' % (fresh_file_full_path_today, ref_file_full_path_today))
        print_horizontal_bar()

    # 3. generate diff files
    for ref_zone_dir, ref_zone_subdirs, ref_zone_files, in sorted(os.walk(referential)):
        if ref_zone_files:
            print('[+] ref zone dir: "%s"' % ref_zone_dir)
            zone_tld_dest_ref_file_full_path_today = os.path.join(ref_zone_dir, 'today.gz')
            zone_tld_dest_ref_file_full_path_today_new = os.path.join(ref_zone_dir, 'today_new.gz')
            zone_tld_dest_ref_file_full_path_yesterday = os.path.join(ref_zone_dir, 'yesterday.gz')
            
            print('[+] comparing fresh today.gz with ref yesterday files to find newly created domains')
            cmd_diff = 'bash -c "comm -13 <(zcat \'%s\' | sort -u) <(zcat \'%s\' | sort -u) | tee | gzip > \'%s\'"' % (zone_tld_dest_ref_file_full_path_yesterday, zone_tld_dest_ref_file_full_path_today, zone_tld_dest_ref_file_full_path_today_new)
            exec_diff = exec_cmd(cmd_diff)
            if exec_diff.returncode != 0:
                print('[!] something went wrong while comparing "%s" with "%s"' % (zone_tld_dest_ref_file_full_path_yesterday, zone_tld_dest_ref_file_full_path_today))
            print_horizontal_bar()
    return

def main():
    global parser
    options = parser.parse_args()
    
    if os.path.isdir(os.path.abspath(options.fresh_dir)) and os.path.isdir(os.path.abspath(options.referential_dir)):
        merge(options)
    
    return
    
if __name__ == "__main__" :
    main()
