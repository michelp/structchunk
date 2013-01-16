import os
import time
import uuid
import mmap

from ctypes import *


class Object(Structure):
    """A basic chunk-persisted object.  Consists of a some header
    info, and a key.

    The first bit of the header 'used' in indicates this struct is in
    use.  Clearing the first bit indicates the struct is 'null'.  Note
    that mmap initializes new ranges to all zeros, so db.new() objects
    always start out 'null' until they are db.put().
    """
    _fields_ = [
        ('used', c_uint64, 1),      # bit indicates space is used
        ('version', c_uint64, 15),  # object version
        ('flags', c_uint64, 48),    # user flags
        ('size', c_long),           # size of object
        ('key', c_char * 36),       # object key
        ]

    @classmethod
    def of(cls, *items, **kw):
        """Return new subclass of Object with extended fields.  Note
        this classmethod returns another, dynamic class.
        """
        items = list(items)
        if kw:
            items.extend(kw.items())
        sub = type(cls.__name__, (cls,), dict(_fields_=items))
        sub.size = sizeof(sub)
        return sub

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
        return bytes(self.chunk.mmap[self.pos:self.pos+self.size])


class Array(Object):
    """Sequence protocol shim around an array of objects.
    """

    _fields_ = [
        ('item_size', c_long),
        ]

    @classmethod
    def of(cls, size, item):
        """Create an array of 'item' with a given size.
        """
        sub = super(Array, cls).of(items=item * size)
        sub.item_size = sizeof(item)
        return sub

    def __getitem__(self, index):
        return self.items[index]

    def __setitem__(self, index, value):
        self.items[index] = value

    def __len__(self):
        return self.items._length_

    def __iter__(self):
        return iter(self.items)

    def __repr__(self):
        return 'Array('+ str(len(self)) + ', ' + repr(list(self)) + ')'


class Chunk(Object):
    """File-backed object container.

    This structure maps to the absolue zero position of a chunk file.
    All sub-objects in chunks are offset into the Chunk file at some
    position.
    """
    _fields_ = [
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

