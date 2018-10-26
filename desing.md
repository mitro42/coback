# COnsolidate BACKups

Consolidate multiple folder of backups, backups of backups, partial copies and so on to a single location with minimal headache.
Multiple source folders with potentially lots of duplicates and lots ot rubbish, one target location.

## Goals

- Have a single location where any number of files can be stored
- Do not have to make a decision about the same file twice
  - If it was already copied to the target folder, don't show it again
  - If it was previously considered and deleted, remember that decision and don't show it again
- The process should be interruptable at any time and picking up later and continue where left off should be as simple as possible
- It should be easy to verify if a folder was already completely processed

## The Tool

Coback is a tool that helps with this. For every folder it procecces it creates a catalog file (only for the top level, not for each of its sub-folders).
The catalog contains

- the status of the folder in the whole process (eg. cataloging started, copying files, folder processed, etc.)
- one entry for each file the folder contains (once the cataloging is done)

A catalog entry contains

- file name and path relative to the folder
- size in bytes
- last modification timestamp
- md5sum of the file
- was the file deleted _TODO: define the exact mechanism to delete a file and to mark it as deleted_

## Catalog states

- `initializing`
- `initialized`
- `incomplete`
- `copying`
- `copied`
- `done`
- `corrupted`

_TODO: are the `incomplete` and `initializing` states really different?_

## Interface

###

Ideally there should be only one operation:

```
$ coback folder_to_import staging_folder target_folder
```

The tool should take care of all the housekeeping, and if absolutely necessary ask the user for decision.

### The general workflow should look like this

The user has 3 folders (A, B, C) with some pictures in them. The goal is to move all images the the target folder (T), ignore duplicates found in A, B and C, and the deleted files shouldn't creep back in. During the process a temporary staging folder (S) will be used.
Coback creates one catalog file for each of A, B, C, S and T. Apart from this file, it never modifies or deletes any file in A, B, C and T.
It can delete files from S.

1.  the user runs `$ coback A S T`
2.  coback creates the empty S and T folders and copies the contents of A to S (if two or more files anywhere in A have the same md5sum, only one will be copied to S)
3.  after coback finishes the user goes through the files in S and copies or moves them over to T. File can also be deleted from S.
4.  the user runs `$ coback B S T`
5.  coback checks the changes in the contents of S and T, and copies the contents of B to S. The files present in S or T are skipped. The files previously deleted from S are also skipped.
6.  same as 3., the user goes through the contents of S.
7.  the user runs `$ coback C S T`
8.  same as 5, except with C.
9.  same as 3., the user goes through the contents of S.
10. Once all file in S have been moved or copied to T or deleted, the process is done. All contents of A, B, and C are available in T with the rubbish removed. A, B and C can be deleted.

Notes:

- If the user modifies the catalog files, or modifies the contents of any of the folders while the tool is running, no consistency is guaranteed
- Once the process is completed the catalog in T should contain an entry for every file in all the input folders. If `$ coback A S T` is run again, it will not create any new files in S, instead it will tell the user that all non-rubbish content in A is already present in T.
- It is not required that S is emptied at each manual step. It is perfectly accepted if the user runs all three command together and starts processing S after they finished. In this case, just as before the contents of S will always be unique, no duplicates will be copied to S.
- It is okay if the user creates duplicates in T. Those will not affect the operations in the future.
- If a file is deleted from T during the manual step, it will be remembered as deleted and if encountered again it won't be copied to S.
- If a file is modified in T during the manual step, and the original is not kept in T, the original will be treated and rubbish and if encountered again it won't be copied to S.
- If a file is modified in T during the manual step, and the original is still present in T, it won't be removed and the two files will be treated as unrelated, non-rubbish files.
-

## Low level operations

These are the lower level steps the normal process is built on. It shouldn't be necessary to use them under normal circumstances.

### Catalog operations

- create/recreate

  - perform a full scan of the folder, calculate all the checksums, and writes a new catalog file.

  If a catalog file previously existed, it overwritten.
  During the process the state is `initializing`, once it's finished the state is `initialized`.
  If the original catalog was complete and hasn't been corrupted, the new catalog should be the same except for the state.

- quick check

  - checks the list of files in the folder and its subfolders and compares the file name and path, the last modification timestamp and the file size to the values in the catalog
  - if the said values are the same it assumes the checksums are correct too. _TODO: state transitions_
  - if the folder contains files missing from the catalog, its state is changed to incomplete _TODO: is that all?_
  - if the values don't match, e.g a file's modification time is different from the one stored in the catalog, the state is changed to `corrupted`

- deep check
  - recalculates all the checksums for the contents of the folder
  - if a mismatch is found, it stops and the state is changed to `corrupted`
  - if a missing file is fount, it stops and the state is changed to `incomplete`
  - if everything matches the state is not changed

### File operations

- copy
  Performs a quick check. If the status is `initalized`, it starts copying the files to the staging folder. _TODO: what if the state is something else?_
  A file can be ignored (not copied) if

  - it is already in the stating folder
  - it is already in the target folder
  - it was already deleted from the target folder
    If succesfully finished the state chanded to `copied`

- copy check _TODO: find a better name, check becomes too overloaded_
  Checks the current folder agains the target folder (???)

## Questions

- Should the tool be interactive?

Probably yes. If quick check finds new files is should ask the user 'Do you want to update the catalog with the new files? Y/n'
It's better user experience than just reporting the problem, and letting the user to find the next step.

- Should there be a completely non-interactive option? E.g by supplying default answers as command line operations

Probably no, at least not at first
