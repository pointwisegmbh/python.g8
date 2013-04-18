#!/usr/bin/python
import sys
sys.path.append('utilities/')

import logging

import appUtils

def usageString():
    return '%prog: Insert usage instructions here'

def versionString():
    return '%prog: 1.0'

def appSetOpts(parser):
    # add your app specific options here,
    # run ./pythonSkeletonApp.py -h for the default options

    return

def main(options, args, parser):

    # Your app starts here
    logging.info("Hello World!")    

    return

if __name__ == '__main__':
    appUtils.main(usageString, versionString, appSetOpts, main)
