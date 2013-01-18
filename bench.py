import sys
import time
from random import randint, random, choice
from uuid import uuid1
from structchunk.db import *
from structchunk.types import *

test_size = 256

i_test = Array.of(test_size, Array.of(test_size, c_int))
d_test = Array.of(test_size, Array.of(test_size, c_double))
o_test = Array.of(test_size, Array.of(test_size, Object.of(iint=c_int, idouble=c_double)))
r_test = Ring.of(32, c_int)
randi = lambda: randint(0, 127)

O = Object.of(a=c_int, b=c_double)
R = Ring.of(32, O)

def i_func(item, i, j):
    item[i][j] = randi()


def d_func(item, i, j):
    item[i][j] = randi()


def o_func(item, i, j):
    i = item[i][j]
    i.iint = randi()
    i.idouble = random()


def randoms(db):
    for typ, func, name in ((i_test, i_func, 'ints'),
                            (d_test, d_func, 'doubles'),
                            (o_test, o_func, 'objects')):
        print "random %s writes" % name
        obs = 0
        arrays = []
        keys = []
        start = time.time()
        for t in xrange(10):
            item = db.new(typ, sync=False)
            for i in xrange(test_size):
                for j in xrange(test_size):
                    i, j = randi(), randi()
                    keys.append((i, j))
                    func(item, i, j)
            db.put(str(uuid1()), item, sync=False)
            obs += test_size * test_size
            arrays.append(item)
        duration = time.time() - start
        print "    total time %s for %s obs (%s writes/s)" % (duration, obs, obs/duration)

        for i in range(2):
            print "random %s reads" % name
            obs = 0
            start = time.time()
            for item in arrays:
                for i in xrange(test_size):
                    for j in xrange(test_size):
                        i, j = choice(keys)
                        x = item[i][j]
                db.put(str(uuid1()), item)
                obs += test_size * test_size
            duration = time.time() - start
            print "    total time %s for %s obs (%s reads/s)" % (duration, obs, obs/duration)
        print


def sequentials(db):
    for typ, func, name in ((i_test, i_func, 'ints'),
                            (d_test, d_func, 'doubles'),
                            (o_test, o_func, 'objects')):
        print "sequential %s writes" % name
        obs = 0
        arrays = []
        start = time.time()
        for t in xrange(10):
            item = db.new(typ, sync=False)
            for i in xrange(test_size):
                for j in xrange(test_size):
                    func(item, i, j)
            db.put(str(uuid1()), item, sync=False)
            obs += test_size * test_size
            arrays.append(item)
        duration = time.time() - start
        print "    total time %s for %s obs (%s writes/s)" % (duration, obs, obs/duration)

        for i in range(2):
            print "sequential %s reads" % name
            obs = 0
            start = time.time()
            for item in arrays:
                for i in xrange(test_size):
                    for j in xrange(test_size):
                        x = item[i][j]
                db.put(str(uuid1()), item)
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
    if len(sys.argv) > 2 and sys.argv[2] == 'bench':
        randoms(db)
        sequentials(db)
