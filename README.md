# ZipROFS
[![Build Status](https://travis-ci.com/openscopeproject/ZipROFS.svg?branch=dev)](https://travis-ci.com/openscopeproject/ZipROFS)

ZipROFS is a FUSE filesystem that acts as pass through to another FS except it
expands zip files like folders and allows direct transparent access to the contents.

We modified ZipROFS according to the needs of brukertimstof mass spectrometry files.
Our mass spectrometry records are stored in ZIP files:

 <DIV style="padding:1em;border:2px solid gray;float:left;">
                     File tree with zip files on hard disk:
       <PRE style="font-family: monospace,courier,ariel,sans-serif;">
 ├── <B style="color:#1111FF;">brukertimstof</B>
 │   └── <B style="color:#1111FF;">202302</B>
 │       ├── 20230209_hsapiens_Sample_001.d.Zip
 │       ├── 20230209_hsapiens_Sample_002.d.Zip
 │       └── 20230209_hsapiens_Sample_003.d.Zip

 ...
 </PRE>
 </DIV>

With the original version of ZipROFS we would see folders ending with <i>.d.Zip</i>.
However, the software requires folders ending with <i>.d</i> like this:


 <DIV style="padding:1em;border:2px solid gray;float:right;">
             Virtual file tree presented by ZipROFS:
             <PRE style="font-family: monospace,courier,ariel,sans-serif;">
 ├── <B style="color:#1111FF;">brukertimstof</B>
 │   └── <B style="color:#1111FF;">202302</B>
 │       ├── <B style="color:#1111FF;">20230209_hsapiens_Sample_001.d</B>
 │       │   ├── analysis.tdf
 │       │   └── analysis.tdf_bin
 │       ├── <B style="color:#1111FF;">20230209_hsapiens_Sample_002.d</B>
 │       │   ├── analysis.tdf
 │       │   └── analysis.tdf_bin
 │       └── <B style="color:#1111FF;">20230209_hsapiens_Sample_003.d</B>
 │           ├── analysis.tdf
 │           └── analysis.tdf_bin

 </PRE>
 </DIV>


A current problem is that computation is slowed down with ZipROFS compared to conventional file systems.

The reason lies within the closed source shared library <i>timsdata.dll</i>.  Reading proprietary
mass spectrometry files with this library creates a huge amount of file system requests.
Furthermore file reading is not sequential.

To solve the performance problem, we

 - Reimplement ZipROFS using the language C [ZIPsFS](https://github.com/christophgil/ZIPsFS).

 - Catch calls to the file API using the LD_PRELOAD method: [cache_readdir_stat](https://github.com/christophgil/cache_readdir_stat)


### Dependencies
* FUSE
* fusepy

### Limitations
* Read only
* Nested zip files are not expanded, they are still just files


### Example usage
To mount run ziprofs.py:
```shell
$ ./ziprofs.py ~/root ~/mount -o allowother,cachesize=2048
```

Example results:
```shell
$ tree root
root
├── folder
├── test.zip
└── text.txt

$ tree mount
mount
├── folder
├── test.zip
│   ├── folder
│   │   ├── emptyfile
│   │   └── subfolder
│   │       └── file.txt
│   ├── script.sh
│   └── text.txt
└── text.txt
```

You can later unmount it using:
```shell
$ fusermount -u ~/mount
```

Or:
```shell
$ umount ~/mount
```

Full help:
```shell
$ ./ziprofs.py -h
usage: ziprofs.py [-h] [-o options] [root] [mountpoint]

ZipROFS read only transparent zip filesystem.

positional arguments:
  root        filesystem root (default: None)
  mountpoint  filesystem mount point (default: None)

optional arguments:
  -h, --help  show this help message and exit
  -o options  comma separated list of options: foreground, debug, allowother, async, cachesize=N (default: {})
```

`foreground` and `allowother` options are passed to FUSE directly.

`debug` option is used to print all syscall details to stdout.

By default ZipROFS disables async reads to improve performance since async syscalls can
be reordered in fuse which heavily impacts read speeds.
If async reads are preferable, pass `async` option on mount.

`cachesize` option determines in memory zipfile cache size, defaults to 1000
