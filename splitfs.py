#!/usr/bin/python2

import re
import os
import sys
import stat
import errno
import fcntl
import struct
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

def getblocksize(path):
    req = 0x80081272 # BLKGETSIZE64 on Linux
    buf = ' ' * 8
    with open(path) as dev:
        buf = fcntl.ioctl(dev.fileno(), req, buf)
    return struct.unpack('L', buf)[0]

# XXX: LOCK it
def refresh_stat(func):
    @functools.wraps(func)
    def wrapped(self, *args, **kwargs):
        path = self.src_path
        for i in xrange(10):
            st = os.lstat(path)
            if stat.S_ISBLK(st.st_mode):
                size = getblocksize(path)
                if size == 0: raise FuseOSError(errno.ENOENT)
                mode = (stat.S_IFREG | stat.S_IMODE(st.st_mode))
                break
            elif stat.S_ISREG(st.st_mode):
                size = st.st_size
                mode = st.st_mode
                break
            elif stat.S_ISLNK(st.st_mode):
                path = os.readlink(path)
            else:
                #raise FuseOSError(errno.EACCES)
                raise FuseOSError(errno.ENOENT)
        else:
            print "Warning: following link for too much time, aborted."
            raise FuseOSError(errno.ENOENT)

        self.src_stat = dict((key, getattr(st, key)) for key in (
            'st_atime', 'st_ctime', 'st_mtime',
            'st_uid', 'st_gid', 'st_nlink',
        ))
        self.src_stat['st_size'] = size
        self.src_stat['st_mode'] = mode
        return func(self, *args, **kwargs)
    return wrapped


#class SplitFS(LoggingMixIn, Operations):
class SplitFS(Operations):
    def __init__(self, src, chunk_size):
        self.src_stat = None
        self.src_path = src
        self.src_name = os.path.basename(src)

        self.chunk_size = parseSize(chunk_size)

        print "self.chunk_size = %d" % self.chunk_size

    def get_piece_range(self, n):
        src_size = self.src_stat['st_size']
        start = n * self.chunk_size
        assert start < src_size
        end = min((n + 1) * self.chunk_size, src_size) - 1
        size = end - start + 1
        return (start, end, size)

    # access() should make sure the source file exists
    @refresh_stat
    def access(self, path, mode):
        return 0

    def get_n(self, path):
        fn = os.path.basename(path)
        l = fn.rsplit('.', 1)
        if len(l) != 2 or l[0] != self.src_name or l[1] == '-0':
            raise FuseOSError(errno.ENOENT)
        try:
            n = int(l[1])
        except ValueError:
            raise FuseOSError(errno.ENOENT)

        npieces = -(-self.src_stat['st_size'] / self.chunk_size) # safe ceiling div
        if n >= npieces or n < 0:
            raise FuseOSError(errno.ENOENT)
        return n

    @refresh_stat
    def getattr(self, path, fh=None):
        st = self.src_stat.copy()

        if path[-1] == '/':
            st['st_nlink'] = 2
            st['st_size'] = 4096
            st['st_mode'] = (stat.S_IFDIR | stat.S_IMODE(st['st_mode']) | 0o111)
        else:
            n = self.get_n(path)
            _, _, size = self.get_piece_range(n)
            st['st_size'] = size

        return st

    def open(self, path, flags):
        #if flags & (os.O_RDWR | os.O_WRONLY):
        #    raise FuseOSError(errno.EROFS)

        # Just checking the path is valid
        self.get_n(path)

        fh = os.open(self.src_path, flags)
        print "open: %s => %d" % (path, fh)
        return fh

    def release(self, path, fh):
        os.close(fh)
        return 0

    @refresh_stat
    def readdir(self, path, fh):
        npieces = -(-self.src_stat['st_size'] / self.chunk_size) # safe ceiling div

        return ['.', '..'] + ['%s.%d' % (self.src_name, i) for i in xrange(npieces)]

    @refresh_stat
    def read(self, path, size, offset, fh):
        n = self.get_n(path)

        start, end, piece_size = self.get_piece_range(n)
        #assert offset + size <= piece_size
        #print "read: fh=%d size=%d offset=%d => [%s, %s]" % (fh, size, offset, start, end)

        os.lseek(fh, start + offset, 0)
        return os.read(fh, size)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "Usage: %s <source> <mountpoint> [chunksize]" % sys.argv[0]
        exit(-1)

    #logging.basicConfig(level=logging.DEBUG)

    FUSE(
        SplitFS(
            sys.argv[1],
            sys.argv[3] if len(sys.argv) == 4 else DEFAULT_CHUNK_SIZE,
        ),
        sys.argv[2],
        fsname='splitfs',
        ro=True,
        foreground=True,
        nothreads=True,
    )

