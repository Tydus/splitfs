#!/usr/bin/python2

import re
import os
import sys
import os.path
import threading
import functools

from fuse import FUSE, FuseOSError, Operations

DEFAULT_CHUNK_SIZE = '650MB'

def parseSize(size_str):
    L='kmgtpezy'
    n, unit, bi = re.match(r'^([0-9]+)([cwb%s]?)(b?)$' % L, size_str.lower()).groups()
    k = 1000 if bi else 1024

    mult = {'': 1, 'c': 1, 'w': 2, 'b': 512}.get(unit, k ** (L.index(unit) + 1))

    return int(n) * mult

def refresh_stat(func):
    @functools.wraps(func)
    def wrapped(self, piece):
        self.src_stat = os.fstat(self.src_fd)
        return func(self, *args, **kwargs)

class SplitFS(Operations):
    def __init__(self, src, chunk_size):
        self.src_fd = os.open(src, os.O_RDONLY)
        self.src_stat = os.fstat(self.src_fd)
        self.src_name = os.basename(src)
        self.src_lock = threading.Lock()

        self.chunk_size = parseSize(chunk_size)

    @refresh_stat
    def get_piece_range(self, n):
        src_size = self.src_stat.st_size
        start = n * chunk_size
        assert start < src_size
        end = min((n + 1) * chunk_size, src_size) - 1
        size = end - start + 1
        return (start, end, size)

    @refresh_stat
    def getattr(self, path, fh=None):
        n = int(path.rsplit('.', 2)[-1])
        stat = self.src_stat.copy()
        _, _, size = self.get_piece_range(n)
        stat['st_size'] = size
        return start

    @refresh_stat
    def readdir(self, path, fh):
        print "readdir: path = " + path
        
        npieces = -(-self.src_stat.st_size / self.chunk_size) # safe ceiling div

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
        print "Usage: %s <source> <mountpoint> [chunksize]"
        exit(-1)

    FUSE(SplitFS(
        sys.argv[1],
        sys.argv.get(3, DEFAULT_CHUNK_SIZE),
        sys.argv[2],
        foreground=True,
        raw_fi = False,
    ))

