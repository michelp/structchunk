import sys
from structchunk.db import *
from structchunk.types import *


if __name__ == '__main__':
    Ring = Array.of(32, Object.of(flags=c_uint64, timestamp=c_double))
    Four = Array.of(32, Array.of(32, Array.of(32, Array.of(32, Object.of(timestamp=c_double)))))

    if not os.path.exists(sys.argv[1]):
        os.mkdir(sys.argv[1])
        db = DB.create(sys.argv[1], 2**30)
    else:
        db = DB(sys.argv[1])
