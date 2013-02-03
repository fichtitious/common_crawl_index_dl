#!/usr/bin/env python

'''Based on https://github.com/trivio/common_crawl_index.'''

import os
import sys
import boto
from cStringIO import StringIO

sys.path.append(os.path.join(os.path.dirname(__file__), 'common_crawl_index/bin'))
from remote_read import BotoMap

from lib.pbtree import PBTreeDictReader

def main(domain, localDir):

  mmap = BotoMap(
    'aws-publicdatasets',
    '/common-crawl/projects/url-index/url-index.1356128792'
  )

  reader = PBTreeDictReader(
    mmap,
    value_format='<QQIQI',
    item_keys=(
      'arcSourceSegmentId',
      'arcFileDate',
      'arcFilePartition',
      'arcFileOffset',
      'compressedSize'
    )
  )

  cx = boto.connect_s3(anon=True)
  bucket = cx.lookup('aws-publicdatasets')

  for url, info in reader.itemsiter(domain):

    localFile = os.path.join(localDir, '-'.join(map(str, (info['arcSourceSegmentId'],
                                                          info['arcFileDate'],
                                                          info['arcFilePartition'],
                                                          info['arcFileOffset'])))) + '.GZ.Z'
    if os.path.exists(localFile):
      print '%s already exists' % localFile
    else:
      try:
        keyname = '/common-crawl/parse-output/segment/{arcSourceSegmentId}/{arcFileDate}_{arcFilePartition}.arc.gz'.format(**info)
        key = bucket.lookup(keyname)
        start = info['arcFileOffset']
        end = start + info['compressedSize'] - 1
        headers = {'Range' : 'bytes={}-{}'.format(start, end)}
        chunk = StringIO(key.get_contents_as_string(headers=headers))
      finally:
        dest = open(localFile, 'w')
        for line in chunk:
          dest.write(line)
        print '%s written' % localFile

if __name__ == '__main__':
  main(domain = sys.argv[1],
       localDir=sys.argv[2])
