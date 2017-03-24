The code in this directory loads the blobs and logs based on a per blob basis

When loading an external table by pointing directly to a file, in some cases I was getting a weird internal error: Internal error. The string routine in file d:\b\s3\sources\sql\ntdbms\query\qeoptim\util\testhookxml.cpp, line 966 failed with HRESULT 0x8007007a.

We are currently loading on a per directory basis, but this code is here to rebuild everything if we decide to go on a per file basis.

The difference is with a per directory basis, if a daily load fails for a table based on a single file, the entire day is not loaded, and on a per file basis, if only one file in a directory failed, at least some data for that day would be loaded.