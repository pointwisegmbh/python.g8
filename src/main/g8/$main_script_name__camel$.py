#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import argparse
import json
import $name;format="word"$


LOGGER = logging.getLogger(__name__)


def main(conf):
    pass


if __name__ == '__main__':
    LOGGER.info("Start")

    parser = argparse.ArgumentParser(description='$desc$')

    parser.add_argument('--config', required=False,
                        help="Config file")

    parser.add_argument('--debug', required=False, help="Debug mode", action="store_true",
                        default=False)

    cmd_args = parser.parse_args()

    debug = cmd_args.debug
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    conf = {}

    with open(cmd_args.config) as json_file:
        conf = json.load(json_file)

    main(conf)
