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

# Script version
VERSION = '1.0'

# Options definition
parser = argparse.ArgumentParser(description="version: " + VERSION)

input_group = parser.add_argument_group('Input parameters')
input_group.add_argument('-f', '--fresh-dir', help='Input directory of the fresh data to merge', default = None, required = True)
input_group.add_argument('-r', '--referential-dir', help='Referential directory', default = None, required = True)
input_group.add_argument('-d', '--diff-file', help='Differential file', default = None, required = True)

def exec_cmd(cmd):
    # yes, I know, <shell=True>
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    pprint.pprint(p.args)
    pprint.pprint("RETCODE: %s"+ p.returncode)
    pprint.pprint("STDOUT:\n" + p.stdout.splitlines())
    pprint.pprint("STDERR:\n" + p.stderr.splitlines())
    
    

def merge(options):
    print("[+] fresh dir:\t\t'%s'" % options.fresh_dir)
    print("[+] referential dir:\t'%s'" % options.referential_dir)
    
    fresh = options.fresh_dir
    referential = options.referential_dir
    
    
    for fresh_dir in sorted(os.listdir(fresh)):
        #print(fresh_directory)
        fresh_dir_full_path = os.path.join(fresh, fresh_dir)
        ref_dir_full_path = os.path.join(referential, fresh_dir)
        
        if os.path.isdir(fresh_dir_full_path) and not(os.path.exists(ref_dir_full_path)):
            print("[!] this subfolder exists in fresh but not in ref:\t'%s'" % (fresh_dir))
            #os.makedirs(ref_dir_full_path)
        
        for fresh_file in sorted(os.listdir(fresh_dir_full_path)):
            fresh_file_full_path = os.path.join(fresh_dir_full_path, fresh_file)
            ref_file_full_path = os.path.join(ref_dir_full_path, fresh_file)
            
            if os.path.isfile(fresh_file_full_path) and not(os.path.exists(ref_file_full_path)):
                print("[!] this subfile exists in fresh but not in ref:\t'%s'" % (fresh_file_full_path))
                #shutil.copyfile(fresh_file_full_path, ref_file_full_path)
                
            else:
                cmd = 'bash -c "comm -13 <(zcat \'%s\' | sort -u) <(zcat \'%s\' | sort -u) >> \'%s\'"' % (ref_file_full_path, fresh_file_full_path, os.path.abspath(options.diff_file))
                exec_cmd(cmd)
    
    
    return

def main():
    global parser
    options = parser.parse_args()
    
    if os.path.isdir(options.fresh_dir) and os.path.isdir(options.referential_dir):
        merge(options)
    
    return
    
if __name__ == "__main__" :
    main()