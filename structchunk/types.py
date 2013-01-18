import os
import time
import uuid
import mmap
from operator import attrgetter

from ctypes import *


c_uuid = c_char * 36


class Object(Structure):
    """A basic chunk-persisted object.

    Subclasses can extend this class to provide custom fields that can
    be any ctypes compatible type.

    The first bit of the header 'used' in indicates this struct is in
    use.  Clearing the first bit indicates the struct is 'null'.  Note
    that mmap initializes new ranges to all zeros, so db.new() objects
    always start out 'null' until they are db.put().
    """
    __slots__ = ('chunk', 'pos')

    _fields_ = [
        ('used', c_uint64, 1),      # bit indicates space is used
        ('version', c_uint64, 15),  # object version
        ('flags', c_uint64, 48),    # user flags
        ('size', c_long),           # computed size of object
        ]

    @classmethod
    def of(cls, *items, **kw):
        """Return new subclass of Object with extended fields.  Note
        this classmethod returns new dynamic subclass of 'cls'.
        """
        items = list(items)
        if kw:
            items.extend(kw.items())
        return type(cls.__name__, (cls,), dict(__slots__=cls.__slots__,
                                               _fields_=items))

    @classmethod
    def from_chunk(cls, chunk, pos):
        """Create a new Object from a chunk at the given position.
        """
        ob = cls.from_buffer(chunk.mmap, pos)
        ob.chunk = chunk
        ob.pos = pos
        return ob

    @property
    def bytes(self):
        """Return the struct as bytes. Useful for debugging. """
        return bytes(self.chunk.mmap[self.pos:self.pos+sizeof(self)])


class Array(Object):
    """Sequence protocol shim around an array of objects.
    """
    __slots__ = ('chunk', 'pos')

    @classmethod
    def of(cls, size, item, *args, **kw):
        """Create an array of 'item' with a given size.
        """
        kw['items'] = item * size
        return super(Array, cls).of(*args, **kw)

    def  __getitem__(self, index):
        return self.items[index]

    def __setitem__(self, index, value):
        self.items[index] = value

    def __delitem__(self, index):
        item = self.items[index]
        if not isinstance(item, Object):
            raise TypeError("Cannot delete item of type %s" % type(item))
        self.items[index].used = False

    def __len__(self):
        return self.items._length_

    def __iter__(self):
        return iter(self.items)

    def __reversed__(self):
        return reversed(self.items)


class Ring(Array):
    """ A ring buffer.

    Keeps a 'head' field that indicates the current zeroth position of
    the ring.  Prepend pushes a new item to the 'front' of the ring,
    and 'append' pushes a new item to the 'back'.  If the ring is
    full, one end or the other will be overwritten.  'get' retrieves
    items indexed relative to the head.  'forward' and 'reverse' are
    used to iterate the ring, relative to the head.
    """
    _fields_ = [
        ('head', c_int),
        ]

    def prepend(self, item):
        self.head = (self.head - 1) % len(self)
        if isinstance(item, Object):
            item.used = True
        self.items[self.head] = item

    def append(self, item):
        if isinstance(item, Object):
            item.used = True
        self.items[self.head] = item
        self.head = (self.head + 1) % len(self)

    def __getitem__(self, offset):
        return Array.__getitem__(self, (self.head + offset) % len(self))

    def __iter__(self):
        for i in xrange(self.head, self.head + len(self), 1):
            yield item

    def __reversed__(self):
        for i in xrange(self.head, self.head - len(self), -1):
            yield item

class Chunk(Object):
    """File-backed object container.

    This structure maps to the absolue zero position of a chunk file.
    All sub-objects in chunks are offset into the Chunk file at some
    position.
    """
    __slots__ = ('chunk', 'mmap',)

    _fields_ = [
        # Object header is included
        # due to subclassing
        ('key', c_uuid),        # object key
        ('head', c_long),       # the start of free space
        ('created', c_double),  # when the chunk was created
        ]

    @classmethod
    def from_mmap(cls, cmap, pos=0):
        chunk = cls.from_buffer(cmap, pos)
        chunk.mmap = cmap
        chunk.chunk = chunk
        return chunk

    @classmethod
    def from_file(cls, filename):
        """ Create a new chunk object from an existing file.
        """
        cfile = open(filename, 'r+b')
        return cls.from_mmap(mmap.mmap(cfile.fileno(), 0))

    @classmethod
    def create(cls, size, chunksdir, key=None, key_func=uuid.uuid1):
        """Create a new chunk file of 'size' in 'chunksdir'.

        If no key is specified, key_func is called for the key, which
        defaults to a uuid.uuid1 constructor.
        """
        if size < sizeof(cls):
            raise TypeError('Chunk must be at least %s size' % size)
        if isinstance(key, basestring):
            key = uuid.UUID(key)
        key = key if key else key_func()

        # try to make a sparse file
        chunkfile = os.path.join(chunksdir, str(key))
        cfile = open(chunkfile, 'ab')
        cfile.truncate(size)
        cfile.close()
        chunk = cls.from_file(chunkfile)
        chunk.used = 1
        chunk.key = str(key)
        chunk.size = size
        chunk.created = time.time()
        chunk.head = sizeof(cls)
        chunk.flush()
        return chunk

    def flush(self):
        return self.mmap.flush()

    def close(self):
        return self.mmap.close()

