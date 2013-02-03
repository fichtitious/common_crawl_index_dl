#!/bin/sh

set -eux

(
  cd $(dirname $0)
  if [ ! -e common_crawl_index/ ] ; then
    git clone https://github.com/trivio/common_crawl_index.git
    cp common_crawl_index/bin/remote_read common_crawl_index/bin/remote_read.py
  fi
)
