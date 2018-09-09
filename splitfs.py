#!/usr/bin/python2

import re
import os
import sys
import stat
import logging
import os.path
import threading
import functools

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

DEFAULT_CHUNK_SIZE = '650MB'

def parseSize(size_str):
    L='kmgtpezy'
    n, unit, bi = re.match(r'^([0-9]+)([cwb%s]?)(b?)$' % L, size_str.lower()).groups()
    k = 1000 if bi else 1024

    mult = {'': 1, 'c': 1, 'w': 2, 'b': 512}.get(unit, k ** (L.index(unit) + 1))

    return int(n) * mult

def refresh_stat(func):
    @functools.wraps(func)
    def wrapped(self, *args, **kwargs):
        st = os.fstat(self.src_fd)
        self.src_stat = dict((key, getattr(st, key)) for key in (
            'st_atime', 'st_ctime', 'st_mtime', 'st_size',
            'st_uid', 'st_gid', 'st_mode', 'st_nlink',
        ))
        return func(self, *args, **kwargs)
    return wrapped

class SplitFS(LoggingMixIn, Operations):
    def __init__(self, src, chunk_size):
        self.src_fd = os.open(src, os.O_RDONLY)
        self.src_stat = None
        self.src_name = os.path.basename(src)
        self.src_lock = threading.Lock()

        self.chunk_size = parseSize(chunk_size)

        print "self.chunk_size = %d" % self.chunk_size

    @refresh_stat
    def get_piece_range(self, n):
        src_size = self.src_stat['st_size']
        start = n * self.chunk_size
        assert start < src_size
        end = min((n + 1) * self.chunk_size, src_size) - 1
        size = end - start + 1
        return (start, end, size)

    @refresh_stat
    def getattr(self, path, fh=None):
        #import pdb; pdb.set_trace()
        st = self.src_stat.copy()

        if path[-1] == '/':
            st['st_size'] = 4096
            st['st_nlink'] = 2
            st['st_mode'] = (stat.S_IFDIR | 0o777)
        else:
            n = int(path.rsplit('.', 2)[-1])
            _, _, size = self.get_piece_range(n)
            st['st_size'] = size

        return st

    @refresh_stat
    def readdir(self, path, fh):
        print "readdir: path = " + path
        
        npieces = -(-self.src_stat['st_size'] / self.chunk_size) # safe ceiling div

        return ['.', '..'] + ['%s.%d' % (self.src_name, i) for i in xrange(npieces)]

    @refresh_stat
    def read(self, path, size, offset):
        n = int(path.rsplit('.', 2)[-1])

        start, end, piece_size = self.get_piece_range(n)
        assert offset + size <= piece_size

        with self.src_lock:
            os.lseek(self.src_fd, start + offset)
            return os.read(self.src_fd, size)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "Usage: %s <source> <mountpoint> [chunksize]" % sys.argv[0]
        exit(-1)

    logging.basicConfig(level=logging.DEBUG)

    FUSE(
        SplitFS(
            sys.argv[1],
            sys.argv[3] if len(sys.argv) == 4 else DEFAULT_CHUNK_SIZE,
        ),
        sys.argv[2],
        foreground=True,
        raw_fi=False,
    )

