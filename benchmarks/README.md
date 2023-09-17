## Benchmarks

### Overview

We use JSON files to detail each benchmark's workload.

For example, `download-256KiB-10_000x.run.json`:
```json
{
    "version": 2,
    "comment": "Use case is an AI company with tons of JPGs",
    "filesOnDisk": true,
    "checksum": null,
    "maxRepeatCount": 10,
    "maxRepeatSecs": 600,
    "tasks": [
        {"action": "download", "key": "download/256KiB/1", "size": 262144},
        {"action": "download", "key": "download/256KiB/2", "size": 262144},
        {"action": "download", "key": "download/256KiB/3", "size": 262144},
        ... etc etc 9994 more lines ...
        {"action": "download","key": "download/256KiB/9998", "size": 262144},
        {"action": "download","key": "download/256KiB/9999", "size": 262144},
        {"action": "download","key": "download/256KiB/10000", "size": 262144}
    ]
}
```

A runner will read a `.run.json` file, perform all tasks, and report how long they all took.

Most benchmarks also have a `.src.json` file that is much more human readable.

For example, `download-256KiB-10_000x.src.json`:
```json
{
    "comment": "Use case is an AI company with tons of JPGs",
    "action": "download",
    "fileSize": "256KiB",
    "numFiles": 10000
}
```

You build the `.src.json` files into `.run.json` by running:
```sh
./aws-crt-s3-benchmarks/scripts/build-benchmarks.py [SRC_FILE ...]
```

You can pass multiple `.src.json` files, or pass none to build everything in `benchmarks/`.

### Design

`.run.json` files are meant to be simple to parse, even though they're annoying to author.
The simplicity is important because we have parsing code written in N different languages.
We don't want to alter N codebases every time we change our mind about a default
value or naming convention.

They're so annoying to author, we came up the idea of separate src files
and a build script.

But you can still write a `.run.json` benchmark without a src file,
if you want to do weird things that aren't officially supported by the build script.
But once we have a bunch of benchmarks doing that weird thing,
consider adding support in the build script.

### Specification: .src.json

`.src.json` files have the following fields. You can omit fields with default values.

*   `comment`: str (default is "").
       A good comment would be the use case that motivated the benchmark.
        Omit if you'd just be repeating the file name.
*   `action`: str. {"upload", "download"}.
       Whether files are uploaded or downloaded in this benchmark.
*   `fileSize`: str. Examples: "5GiB", "8MiB"", "256KiB", "1byte", "0bytes".
       Human readable file size.
*   `numFiles`: int (default is 1).
       Number of files to benchmark. Each file is the same size: `fileSize`.
*   `filesOnDisk`: bool (default is true).
        If true, files are uploaded from disk or downloaded to disk.
        If false, data is just in RAM and never touches the disk.
*   `checksum`: str (default is null). {null, "CRC32", "CRC32C", "SHA1", "SHA256"}.
       If non-null uploads must include this checksum, and downloads must validate the checksum.
*   `maxRepeatCount`: int (default is 10).
       The runner will repeat a benchmark until it reaches `maxRepeatCount`
        or `maxRepeatSecs` seconds elapses.
*   `maxRepeatSecs`: int (default is 600 (10 minutes)).

### Specification: .run.json

All fields are required in a `.run.json` file. Most were described above.

*   `version`: int. {2}. This should be incremented any time a new field or value is added.
*   `comment`: str.
*   `filesOnDisk`: bool.
*   `checksum`: str. {null, "CRC32", "CRC32C", "SHA1", "SHA256"}.
*   `maxRepeatCount`: int.
*   `maxRepeatSecs`: int.
*   `tasks`: array of tasks. Each task contains:
    *   `action`: str. {"upload", "download"}.
    *   `key`: str. Key in S3 bucket to upload or download.
            If `filesOnDisk==true`, this is also the relative file path on disk.
    *   `size`: int. Size of the file in bytes.

