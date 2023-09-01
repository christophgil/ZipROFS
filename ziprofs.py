#!/usr/bin/env python3
from __future__ import print_function, absolute_import, division

from functools import lru_cache

from os.path import realpath
import traceback
import argparse
import ctypes
import errno
import logging
import os
import time
import zipfile
import stat
import threading
from threading import RLock
from typing import Optional, Dict

try:
    from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, S_IFDIR, fuse_operations
    import fuse as fusepy
except ImportError:
    # ubuntu renamed package in repository
    from fusepy import FUSE, FuseOSError, Operations, LoggingMixIn, S_IFDIR, fuse_operations
    import fusepy

from collections import OrderedDict

############################################################################
### Normally, the  the Zip-file paths will be the folder paths.
### This can be changed here.
###
### Here we remove the extension ".Zip" for all files Ending with ".d.Zip"
### The folder names will just end with ".d".
###
### You can implement other conversion rules.
############################################################################
_is_foreground=False
_keep_name_of_zipfile_as_dirname=False
if _keep_name_of_zipfile_as_dirname:
    def len_virtual_zippath(path: str): return len(path)
    def zippath_virtual_to_real(path: str): return path
    def zippath_virtual_to_real_or_none(path: str, end): return None
    def zipfilename_real_to_virtual(name: str): return name
    _skip_ends_with=None
else: # If ending with .d.Zip, then the folder name is .d.
    def len_virtual_zippath(path: str):
        return len(path)-(4 if path.endswith('.d.Zip') else 0)
    def zippath_virtual_to_real(path: str):
        p=zippath_virtual_to_real_or_none(path,len(path)) or path
        #print('\033[42m zippath_virtual_to_real \033[0m '+path+' -> '+p)
        return p
    def zippath_virtual_to_real_or_none(path: str, end):
        end=min(end,len(path))
        if end>3 and path.find('.d',end-2,end)>0:
            z=path[:end]+'.Zip'
            if (os.path.isfile(z)):
                #print('\033[32m zippath_virtual_to_real_or_none \033[0m '+path+' -> '+z)
                return z
        return  None
    def zipfilename_real_to_virtual(name: str): return name[0:-4] if (name.endswith('.d.Zip')) else name
    _skip_ends_with=['/analysis.tdf-journal','/analysis.tdf-wal']

################################
### End mapping folder names ###
################################

@lru_cache(maxsize=2048)
def is_zipfile(path, mtime):
    # mtime just to miss cache on changed files
    return zipfile.is_zipfile(path)


class ZipFile(zipfile.ZipFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__lock = RLock()

    def lock(self):
        return self.__lock


class CachedZipFactory(object):
    MAX_CACHE_SIZE = 1000
    cache = OrderedDict()
    log = logging.getLogger('ziprofs.cache')
    def __init__(self):
        self.__lock = RLock()

    def _add(self, path: str):
        if path in self.cache:
            return
        while len(self.cache) >= self.MAX_CACHE_SIZE:
            oldpath, val = self.cache.popitem(last=False)
            self.log.debug('Popping cache entry: %s', oldpath)
            #if (_is_foreground): print("val="+str(val[1]))
            val[1].close()
        mtime = os.lstat(path).st_mtime
        self.log.debug("Caching path (%s:%s)", path, mtime)
        self.cache[path] = (mtime, ZipFile(path))
    def get(self, path: str) -> ZipFile:
        with self.__lock:
            if path in self.cache:
                self.cache.move_to_end(path)
                mtime = os.lstat(path).st_mtime
                if mtime > self.cache[path][0]:
                    val = self.cache.pop(path)
                    val[1].close()
                    self._add(path)
            else:
                self._add(path)
            return self.cache[path][1]


class ZipROFS(Operations):
    zip_factory = CachedZipFactory()
    _count_getattr=0
    def __init__(self, root):
        self.root = realpath(root)
        # odd file handles are files inside zip, even fhs are system-wide files
        self._zip_file_fh: Dict[int, zipfile.ZipExtFile] = {}
        self._zip_zfile_fh: Dict[int, ZipFile] = {}
        self._fh_locks: Dict[int, RLock] = {}
        self._lock = RLock()

    def __call__(self, op, path, *args):
        return super().__call__(op, self.root + path, *args)

    def _get_free_zip_fh(self):
        i = 5   # avoid confusion with stdin/err/out
        while i in self._zip_file_fh:
            i += 2
        return i

    @staticmethod
    def xxxxxxx_get_zip_path(path: str) -> Optional[str]:
        parts = []
        head, tail = os.path.split(path)
        while tail:
            parts.append(tail)
            head, tail = os.path.split(head)
        parts.reverse()
        cur_path = '/'
        for part in parts:
            cur_path = os.path.join(cur_path, part)
            if (part[-4:] == '.zip' or part[-4:] == '.Zip') and is_zipfile(cur_path,os.lstat(cur_path).st_mtime):
                return cur_path
        return None


    @staticmethod
    def get_zip_path(path: str) -> Optional[str]:  # @CG This overrides get_zip_path
        slash=0
        l=len(path)
        while slash<l:
            slash2=path.find('/',slash+1)
            if (slash2<0): slash2=l
            if (slash<slash2-4):
                cur_path=zippath_virtual_to_real_or_none(path,slash2)
                if (cur_path or path.find('.zip',slash2-4,slash2)>0 or path.find('.Zip',slash2-4,slash2)>0):
                    if not cur_path: cur_path=path[:slash2]
                    #print("cur_path="+cur_path+"  "+str(is_zipfile(cur_path, os.lstat(cur_path).st_mtime)))
                    if (is_zipfile(cur_path, os.lstat(cur_path).st_mtime)): return cur_path
            slash=slash2
        return None


    def access(self, path, mode):
        #if (_is_foreground): print("ziprofs#access "+str(path))
        if ZipROFS.get_zip_path(path): # @CG
            if mode & os.W_OK:
                if (_is_foreground): print("FuseOSError(errno.EROFS)")
                raise FuseOSError(errno.EROFS)
        else:
            if not os.access(path, mode):
                if (_is_foreground): print("FuseOSError(errno.EROFS)")
                raise FuseOSError(errno.EACCES)

    def getattr(self, path, fh=None):
        self._count_getattr=self._count_getattr+1
        debug_orig_path=path
        path=zippath_virtual_to_real(path) ## @CG

        if (_is_foreground and self._count_getattr%1000==0): print(str(self._count_getattr)+' ziprofs#getattr '+str(path)+"  debug_orig_path="+debug_orig_path)
        #traceback.print_exc(limit=None, file=None, chain=True)
        #print('@',end='')
        zip_path = ZipROFS.get_zip_path(path) ## @CG
        st = os.lstat(zip_path) if zip_path else os.lstat(path)
        result = {key: getattr(st, key) for key in (
            'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'
        )}
        if zip_path == path:
            result['st_mode'] = S_IFDIR | (result['st_mode'] & 0o555)
        elif zip_path:
            zf = self.zip_factory.get(zip_path)
            subpath = path[len_virtual_zippath(zip_path) + 1:] ## @CG
            info = None
            try:
                info = zf.getinfo(subpath)
                result['st_size'] = info.file_size
                result['st_mode'] = stat.S_IFREG | 0o555
            except KeyError:
                # check if it is a valid subdirectory
                try:
                    info = zf.getinfo(subpath + '/')
                except KeyError:
                    #if (_is_foreground): print("KeyError "+path)
                    pass
                found = False
                if not info:
                    infolist = zf.infolist()
                    for f in infolist:
                        if f.filename.find(subpath + '/') == 0:
                            found = True
                            break
                if found or info:
                    result['st_mode'] = S_IFDIR | 0o555
                else:
#                    if (_is_foreground): print("FuseOSError(errno.ENOENT")
                    raise FuseOSError(errno.ENOENT)
            if info:
                # update mtime
                try:
                    mtime = time.mktime(info.date_time + (0, 0, -1))
                    result['st_mtime'] = mtime
                except Exception:
                    pass
        return result

    def open(self, path, flags):
        path=zippath_virtual_to_real(path) ## @CG
        zip_path = ZipROFS.get_zip_path(path) ## @CG
        if zip_path:
            with self._lock:
                fh = self._get_free_zip_fh()
                zf = self.zip_factory.get(zip_path)
                self._zip_zfile_fh[fh] = zf
                self._zip_file_fh[fh] = zf.open(path[len_virtual_zippath(zip_path) + 1:]) ## @CG
                return fh
        else:
            fh = os.open(path, flags) << 1
            self._fh_locks[fh] = RLock()
            return fh
    _my_offset=0
    _thread_ident=''
    def read(self, path, size, offset, fh):
        # if (_is_foreground): print('ziprofs#read '+str(path))
        debug_orig_path=path
        path=zippath_virtual_to_real(path) ## @CG
        if fh in self._zip_file_fh:
            # should be here (file is first opened, then read)
            f = self._zip_file_fh[fh]
            with self._zip_zfile_fh[fh].lock():
                if not f.seekable():
                    if (_is_foreground): print("FuseOSError(errno.EBADF")
                    raise FuseOSError(errno.EBADF)
                f.seek(offset)

                try:

                    r=f.read(size)
                    if (self._thread_ident==''): self._thread_ident=str(threading.get_ident());
                    if (path.endswith('tdf_bin') and self._thread_ident==str(threading.get_ident())): # and offset!=self._my_offset):
                        print(str(threading.get_ident())+"  "+path+" size="+str(size)+" r="+str(len(r))+" offset="+str(offset)+" _my_offset="+str(self._my_offset)+"   skip="+str(offset-self._my_offset)+"\n");
                        self._my_offset=offset+len(r)
                    return r
                except EOFError:
                    if (_is_foreground): print("EXCEPT EOFError path="+path+" debug_orig_path="+debug_orig_path+" size="+str(size)+"  offset="+str(offset))
                    exit(999)
                    return None
        else:
            with self._fh_locks[fh]:
                os.lseek(fh >> 1, offset, 0)
                return os.read(fh >> 1, size)

    def readdir(self, path, fh):
        zip_path = ZipROFS.get_zip_path(path) ## @CG
        result = ['.', '..']
        if not zip_path:
            return result+[zipfilename_real_to_virtual(p) for p in os.listdir(path)] ## @CG
        subpath = path[len_virtual_zippath(zip_path) + 1:] ## @CG
        zf = self.zip_factory.get(zip_path)
        infolist = zf.infolist()
        subdirs = set()
        for info in infolist:
            if info.filename.find(subpath) == 0 and info.filename > subpath:
                suffix = info.filename[len(subpath) + 1 if subpath else 0:]
                if not suffix:
                    continue
                if '/' not in suffix:
                    result.append(suffix)
                else:
                    subdirs.add(suffix[:suffix.find('/')])
        result.extend(subdirs)
        return result

    def release(self, path, fh):
        if fh in self._zip_file_fh:
            with self._lock:
                f = self._zip_file_fh[fh]
                with self._zip_zfile_fh[fh].lock():
                    del self._zip_file_fh[fh]
                    del self._zip_zfile_fh[fh]
                    return f.close()
        else:
            with self._fh_locks[fh]:
                del self._fh_locks[fh]
                return os.close(fh >> 1)

    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in (
            'f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail',
            'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'
        ))


class ZipROFSDebug(LoggingMixIn, ZipROFS):
    def __call__(self, op, path, *args):
        return super().__call__(op, self.root + path, *args)


class fuse_conn_info(ctypes.Structure):
    _fields_ = [
        ('proto_major', ctypes.c_uint),
        ('proto_minor', ctypes.c_uint),
        ('async_read', ctypes.c_uint),
        ('max_write', ctypes.c_uint),
        ('max_readahead', ctypes.c_uint),
        ('capable', ctypes.c_uint),
        ('want', ctypes.c_uint),
        ('reserved', ctypes.c_uint, 25)]


class ZipROFuse(FUSE):
    def __init__(self, operations, mountpoint, **kwargs):
        self.support_async = kwargs.get('support_async', False)
        del kwargs['support_async']
        if not self.support_async:
            # monkeypatch fuse_operations
            ops = fuse_operations._fields_
            for i in range(len(ops)):
                if ops[i][0] == 'init':
                    ops[i] = (
                        'init',
                        ctypes.CFUNCTYPE(
                            ctypes.c_voidp, ctypes.POINTER(fuse_conn_info))
                    )
                fusepy.fuse_operations = type(
                    'fuse_operations', (ctypes.Structure,), {'_fields_': ops})
        super().__init__(operations, mountpoint, **kwargs)

    def init(self, conn):
        if not self.support_async:
            conn[0].async_read = 0
            conn[0].want = conn.contents.want & ~1
        return self.operations('init', '/')


def parse_mount_opts(in_str):
    opts = {}
    for o in in_str.split(','):
        if '=' in o:
            name, val = o.split('=', 1)
            opts[name] = val
        else:
            opts[o] = True
    return opts


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='ZipROFS read only transparent zip filesystem.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('root', nargs='?', help="filesystem root")
    parser.add_argument(
        'mountpoint',
        nargs='?',
        help="filesystem mount point")
    parser.add_argument(
        '-o', metavar='options', dest='opts',
        help="comma separated list of options: foreground, debug, allowother, async, cachesize=N",
        type=parse_mount_opts, default={})
    arg = parser.parse_args()

    if 'cachesize' in arg.opts:
        cache_size = int(arg.opts['cachesize'])
        if cache_size < 1:
            raise ValueError("Bad cache size")
        CachedZipFactory.MAX_CACHE_SIZE = cache_size

    logging.basicConfig(
        level=logging.DEBUG if 'debug' in arg.opts else logging.INFO)

    if 'debug' in arg.opts:
        fs = ZipROFSDebug(arg.root)
    else:
        fs = ZipROFS(arg.root)
    _is_foreground=('foreground' in arg.opts) ## @CG
    fuse = ZipROFuse(
        fs,
        arg.mountpoint,
        foreground=('foreground' in arg.opts),
        allow_other=('allowother' in arg.opts),
        support_async=('async' in arg.opts)
    )



#     Traceback (most recent call last):
#   File "/local/filesystem/git/ZipROFS/fuse.py", line 734, in _wrapper
#     return func(*args, **kwargs) or 0
#            ^^^^^^^^^^^^^^^^^^^^^
#   File "/local/filesystem/git/ZipROFS/fuse.py", line 847, in read
#     ret = self.operations('read', self._decode_optional_path(path), size, offset, fh)
#           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/local/filesystem/git/ZipROFS/ziprofs.py", line 110, in __call__
#     return super().__call__(op, self.root + path, *args)
#            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/local/filesystem/git/ZipROFS/fuse.py", line 1077, in __call__
#     return getattr(self, op)(*args)
#            ^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/local/filesystem/git/ZipROFS/ziprofs.py", line 233, in read
#     return f.read(size)
#            ^^^^^^^^^^^^
#   File "/local/python/2023_02_cpython-main/Lib/zipfile/__init__.py", line 948, in read
#     data = self._read1(n)
#            ^^^^^^^^^^^^^^
#   File "/local/python/2023_02_cpython-main/Lib/zipfile/__init__.py", line 1018, in _read1
#     data = self._read2(n)
#            ^^^^^^^^^^^^^^
#   File "/local/python/2023_02_cpython-main/Lib/zipfile/__init__.py", line 1051, in _read2
#     raise EOFError
# EOFError
# ERROR:fuse:Uncaught exception from FUSE operation read, returning errno.EINVAL.
# Traceback (most recent call last):
#   File "/local/filesystem/git/ZipROFS/fuse.py", line 734, in _wrapper
