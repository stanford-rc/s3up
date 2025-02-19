#!/usr/bin/env python3

import hashlib
import multiprocessing
import os
import sys

# program name for reporting errors
prog = os.path.basename(sys.argv[0]);

# initial part size is 5 GiB
part_size = 5 * (1024 ** 3);

# documentation for help text
documentation = f'''
NAME

   {prog} -  calculate MD5 Hash and S3 ETag for local files or from stdin

SYNOPSIS

   {prog} [{{-p | --processes}} <processes>] [{{-n | --part-size}} <bytes>] {{- | <file> ...}}

   -h | -help | --help

       Print help and exit.
        
   -p <int> | --processes <int>

       Set the number of processes to launch in parallel.  If not specified the
       number of CPUs is the default.

   -n <bytes> | --part-size <bytes>

       Change the number of bytes per part used for calculating the S3 ETag.
       This flag may be repeated before each file.  The initial default value
       is 5 GiB.

   - | <file> ...

       File(s) to calculate the MD5 Hash and S3 ETag for, or use '-' to read
       once from standard input.

EXAMPLE

   The following shows the output of {prog} for four different files ranging in
   size from 1 to 4 GiB.  A part-size of 500,000,000 (500 MB) is used for x.1
   and a part-size of 1,073,741,824 (1 GiB) is used for x2, x.3, and x.4:

       $ ls -l
       total 10485760
       -rw-r--r--. 1 jimr jimr 1073741824 Feb 18 13:34 x.1
       -rw-r--r--. 1 jimr jimr 2147483648 Feb 18 13:34 x.2
       -rw-r--r--. 1 jimr jimr 3221225472 Feb 18 13:34 x.3
       -rw-r--r--. 1 jimr jimr 4294967296 Feb 18 13:34 x.4

       $ {prog} -n $((500 * (1000 ** 2))) x.1 -n $((1024 ** 3)) x.{{2,3,4}}
       input file      MD5 hash        S3 ETag
       x.1     1e5a631ee8c612596d370f922f1c435a        1f2ec1ae6e884967d08e3c0d7c31f160-3
       x.2     7ba3b0592ecc5713a906334da5e5eaa9        7f146c10464087fa9271cdffda4f35ba-2
       x.3     7993811e4f986046bf3cf89ca67b2575        6af7ce83a80a9e9770967f4c9dfee72a-3
       x.4     9979a256a96edd4537fe8437481b38d8        b4c3229097a1ab335300421b2e580a40-4

   We can verify the MD5 hash manually:

       $ md5sum x.*
       1e5a631ee8c612596d370f922f1c435a  x.1
       7ba3b0592ecc5713a906334da5e5eaa9  x.2
       7993811e4f986046bf3cf89ca67b2575  x.3
       9979a256a96edd4537fe8437481b38d8  x.4

   And if we upload the files using the same part-size values we can verify the
   same MD5 Hash and ETag are generated:

       $ s3up --profile test2-elm -manifest md5 \\
           --part-size $((500 * (1000 ** 2))) \\
           --bucket test-jrobinso x.1; \\
         s3up --profile test2-elm --manifest md5 \\
           --part-size $((1024 ** 3)) \\
           --bucket test-jrobinso x.{{2,3,4}}
       1e5a631ee8c612596d370f922f1c435a  test-jrobinso/x.1
       7ba3b0592ecc5713a906334da5e5eaa9  test-jrobinso/x.2
       7993811e4f986046bf3cf89ca67b2575  test-jrobinso/x.3
       9979a256a96edd4537fe8437481b38d8  test-jrobinso/x.4

       $ for x in $(seq 1 4); do aws --profile test2-elm \\
           s3api get-object-attributes \\
           --bucket test-jrobinso --key x.${{x}} \\
           --object-attributes ETag ; done
       {{
           "LastModified": "2025-02-18T20:09:26+00:00",
           "VersionId": "6bbdc7a7-616f-4fb3-9e0d-0b4ab1187e2e",
           "ETag": "1f2ec1ae6e884967d08e3c0d7c31f160-3"
       }}
       {{
           "LastModified": "2025-02-18T20:11:32+00:00",
           "VersionId": "7fb5b864-d511-44a5-b29b-32760f126522",
           "ETag": "7f146c10464087fa9271cdffda4f35ba-2"
       }}
       {{
           "LastModified": "2025-02-18T20:14:34+00:00",
           "VersionId": "dc8773ff-d54d-4976-b2e7-f8e50e01d200",
           "ETag": "6af7ce83a80a9e9770967f4c9dfee72a-3"
       }}
       {{
           "LastModified": "2025-02-18T20:18:34+00:00",
           "VersionId": "2d797b46-a04e-463c-aabb-ddc9326a558d",
           "ETag": "b4c3229097a1ab335300421b2e580a40-4"
       }}
'''


def chunk(fh, part_size):
    """
    read fh in chunks of part_size
    """
    while True:
        buf = fh.read(part_size)
        if not buf:
            break
        yield buf


def etag(source, part_size, header):
    """
    read source in chunks of part_size and return a summary of the MD5 hash and S3
    ETag
    """

    if source == "-":
        # open file descriptor zero (stdin)
        file = 0
    else:
        # open filesystem path
        file = source

    # open file in binary mode and process it in part_size chunks
    with open(file, mode="rb" ) as fh:
        # MD5 hash, for the whole body hash
        h = hashlib.md5()

        # MD5 hash-of-hashes, for the ETag
        hh = hashlib.md5()

        # track the number of parts
        nparts = 0

        # for each chunk add it to the h and hh hashes
        for buf in chunk(fh, part_size):
            h.update(buf)
            hh.update(hashlib.md5(buf).digest())
            nparts += 1

        # the ETag suffix depends on whether or not multiple parts were used
        if nparts == 1:
            etag = hh.hexdigest()
        else:
            etag = f"{hh.hexdigest()}-{nparts}";

        # optional header
        if header:
            hdr = f"input file\tMD5 hash\tS3 ETag\n"
        else:
            hdr = ""

        # return the MD5 Hash and S3 ETag
        return hdr + f"{source}\t{h.hexdigest()}\t{etag}"


if __name__ == '__main__':
    # skip control processing sys.argv
    skip = False

    # track if heaader should be printed
    header = True

    # default number of parallel processes
    nproc = os.cpu_count()

    # pool will become a multiprocessing.Pool
    pool = None

    # queued will hold async results
    pending = {}

    # loop through sys.argv, setting part_size and processing input files
    for i in range(0,len(sys.argv),1):
        if i == 0 or skip:
            skip = False
            continue
        else:
            skip = False

        if sys.argv[i] == "-h" or sys.argv[i] == "-help" or sys.argv[i] == "--help":
            print(documentation)
            exit(0)
        elif sys.argv[i] == "-p" or sys.argv[i] == "--processes":
            try:
                nproc = int(sys.argv[i+1])
                skip = True
            except:
                print(f"{prog}: unable to parse {sys.argv[i]}: {sys.argv[i+1]}", file=sys.stderr)
                exit(1)
        elif sys.argv[i] == "-n" or sys.argv[i] == "--part-size":
            try:
                part_size = int(sys.argv[i+1])
                skip = True
            except:
                print(f"{prog}: unable to parse {sys.argv[i]}: {sys.argv[i+1]}", file=sys.stderr)
                exit(1)
        else:
            if pool is None:
                pool = multiprocessing.Pool(nproc)

            pending[sys.argv[i]] = pool.apply_async(
                etag, (sys.argv[i], part_size, header))

            header = False

    # nerr tracks the number of errors
    nerr = 0

    # if no files are given, print an error
    if len(pending) == 0:
        nerr += 1
        print(f"{prog}: at least one file argument is required", file=sys.stderr)

    for key, res in pending.items():
        try:
            print(res.get())
        except Exception as err:
            print(f"{prog}: error processing {key}: {err}", file=sys.stderr)
            nerr += 1

    exit(nerr)
