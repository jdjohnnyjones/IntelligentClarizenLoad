The code herein loads the blobs on a per virtual directory basis, instead of on a per file basis; however the logging is done on a per file basis.
The question is whether we want a full day to fail when an issue occurs or just that particular blob

For the test export located in this directory, the load runs in about 5 minutes.

There are some logging issues in that the logging is based on a per file basis, while the load is based on a per directory basis.
This led to some inconsistencies in the start and end times in the log tables where the file name wasn't the first one in the directory.

The reason this is here is because I'm testing doing the loads on a per file basis to compare the times.

If I come back and use this code as the way forward, I should change the logging to a per directory basis to match the load style.