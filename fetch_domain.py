#!/usr/bin/env python

'''Based on https://github.com/trivio/common_crawl_index.'''

import os
import sys
import boto
from cStringIO import StringIO

from lib.pbtree import PBTreeDictReader

class BotoMap(object):
  '''https://github.com/trivio/common_crawl_index/blob/master/bin/remote_read'''

  def __init__(self, bucket, key_name, access_key=None, access_secret=None ):
    if access_key and access_secret:
      self.conn  = boto.connect_s3(access_key,access_secret)
    else:
      self.conn  = boto.connect_s3(anon=True)
    bucket = self.conn.lookup(bucket)
    self.key = bucket.lookup(key_name)
    self.block_size = 2**16
    self.cached_block = -1

  def __getitem__(self, i):
    if isinstance(i, slice):
      start = i.start
      end = i.stop - 1
    else:
      start = i
      end = start + 1
    return self.fetch(start,end)

  def fetch(self, start, end):
    try:
      return self.key.get_contents_as_string(
        headers={'Range' : 'bytes={}-{}'.format(start, end)}
      )
    except boto.exception.S3ResponseError, e:
      # invalid range, we've reached the end of the file
      if e.status == 416:
        return ''

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

    try:
      keyname = '/common-crawl/parse-output/segment/{arcSourceSegmentId}/{arcFileDate}_{arcFilePartition}.arc.gz'.format(**info)
      key = bucket.lookup(keyname)
      start = info['arcFileOffset']
      end = start + info['compressedSize'] - 1
      headers = {'Range' : 'bytes={}-{}'.format(start, end)}
      chunk = StringIO(key.get_contents_as_string(headers=headers))
      localFile = os.path.join(localDir, '-'.join(map(str, (info['arcSourceSegmentId'],
                                                            info['arcFileDate'],
                                                            info['arcFilePartition'],
                                                            info['arcFileOffset'])))) + '.GZ.Z'
    finally:
      if os.path.exists(localFile):
        print '%s already exists' % localFile
      else:
        dest = open(localFile, 'w')
        for line in chunk:
          dest.write(line)
        print '%s written' % localFile

if __name__ == '__main__':
  main(domain = sys.argv[1],
       localDir=sys.argv[2])
