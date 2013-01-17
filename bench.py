import sys
import time
from random import randint, random
from uuid import uuid1
from structchunk.db import *
from structchunk.types import *

test_size = 512

i_test = Array.of(test_size, Array.of(test_size, c_int))
d_test = Array.of(test_size, Array.of(test_size, c_double))
o_test = Array.of(test_size, Array.of(test_size, Object.of(iint=c_int, idouble=c_double)))
randi = lambda: randint(0, 127)


def i_func(item):
    item[randi()][randi()] = randi()


def d_func(item):
    item[randi()][randi()] = randi()


def o_func(item):
    i = item[randi()][randi()]
    i.key = str(uuid1())
    i.iint = randi()
    i.idouble = random()


def randoms(db):
    for typ, func in ((i_test, i_func), (d_test, d_func), (o_test, o_func)):
        print "random writes"
        obs = 0
        arrays = []
        start = time.time()
        for t in xrange(10):
            item = db.new(typ, sync=False)
            for i in xrange(test_size):
                for j in xrange(test_size):
                    func(item)
            db.put(item, sync=False)
            obs += test_size * test_size
            arrays.append(item)
        duration = time.time() - start
        print "    total time %s for %s obs (%s writes/s)" % (duration, obs, obs/duration)

        for i in range(2):
            print "random reads"
            obs = 0
            start = time.time()
            for item in arrays:
                for i in xrange(test_size):
                    for j in xrange(test_size):
                        x = item[randi()][randi()]
                db.put(item)
                obs += test_size * test_size
            duration = time.time() - start
            print "    total time %s for %s obs (%s reads/s)" % (duration, obs, obs/duration)
        print

if __name__ == '__main__':
    path = sys.argv[1]
    if not os.path.exists(path):
        os.mkdir(path)
        db = DB.create(path, 2**30)
    else:
        db = DB(path)
    if len(sys.argv) > 1 and sys.argv[2] == 'bench':
        randoms(db)
