import sys
import time
from random import randint
from structchunk.db import *
from structchunk.types import *

test_size = 32

i_test = Array.of(test_size, Array.of(test_size, c_int))
d_test = Array.of(test_size, Array.of(test_size, c_int))
o_test = Array.of(test_size, Array.of(test_size, Object))

randi = lambda: randint(0, 31)

def write_random(db):
    print "random writes"
    obs = 0
    start = time.time()
    for t in xrange(1000):
        item = db.new(i_test)
        for i in xrange(test_size):
            for j in xrange(test_size):
                item[randi()][randi()] = randint(0, sys.maxint)
        db.put(item)
        obs += test_size * test_size
    duration = time.time() - start
    print "total time %s for %s obs (%s writes/s)" % (duration, obs, obs/duration)


if __name__ == '__main__':
    path = sys.argv[1]
    if not os.path.exists(path):
        os.mkdir(path)
        db = DB.create(path, 2**30)
    else:
        db = DB(path)
    if len(sys.argv) > 1 and sys.argv[2] == 'bench':
        write_random(db)
