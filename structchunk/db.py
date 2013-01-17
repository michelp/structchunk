import os
import time
import uuid

from collections import OrderedDict
from cPickle import dumps, loads

import leveldb

from .types import *


class DB(object):
    """Database for chunk-stored objects.

    Contains an ordered dictionary of chunk objects, each mapped to a
    chunk file.

    Objects are fetched or created through the database.  In either
    case, instances of the passed type are created and backed by the
    file mapped storage.
    """

    @classmethod
    def create(cls, path, chunk_size):
        """Create a new database at 'path' with a chunk size of
        'chunk_size'.
        """
        index_path = os.path.join(path, 'index')
        chunk_path = os.path.join(path, 'chunks')

        if not os.path.exists(index_path):
            os.mkdir(index_path)
        if not os.path.exists(chunk_path):
            os.mkdir(chunk_path)

        index = leveldb.LevelDB(index_path, error_if_exists=True)

        db = cls(path, chunk_size, index)
        db.new_chunk()
        return db

    def __init__(self, path, chunk_size=None, index=None):
        """Open an existing database at 'path'.  If not 'chunk_size'
        is specified, use the size of the newest created chunk in the db.
        """
        self.index_path = os.path.join(path, 'index')
        self.chunk_path = os.path.join(path, 'chunks')
        
        if index is None:
            self.index = index = leveldb.LevelDB(
                self.index_path,
                create_if_missing=False)
        else:
            self.index = index

        self.chunks = OrderedDict()
        self.chunk_size = size = chunk_size
        for name in sorted(os.listdir(self.chunk_path)):
            chunk = Chunk.from_file(os.path.join(self.chunk_path, name))
            assert name == chunk.key
            self.chunks[uuid.UUID(chunk.key)] = chunk
            size = chunk.size

        if size and chunk_size is None:
            self.chunk_size = size
        if self.chunk_size is None:
            raise TypeError('Must pass a chunk size if no chunks to read from.')

    def new_chunk(self, key=None):
        """Create a new chunk and map it into the cache of chunks.
        """
        key = key if key else uuid.uuid1()
        chunk = Chunk.create(self.chunk_size, self.chunk_path, key)
        self.chunks[key] = chunk
        return chunk

    @property
    def chunk(self):
        """ The most recently created chunk. """
        return self.chunks[reversed(self.chunks).next()]

    def flush(self):
        """ Flush all chunks, this calls mmap.flush() for each chunked
        file. """
        for chunk in self.chunks.itervalues():
            chunk.flush()

    def _get_obj_pos(self, key):
        """Helper to get chunk and position from key.

        TODO: use a struct not pickle
        """
        chunk_key, pos = loads(self.index.Get(key))
        chunk = self.chunks[uuid.UUID(chunk_key)]
        return chunk, pos

    def _set_obj_pos(self, key, chunk, pos, sync=True):
        """Helper to set chunk and position for key.

        TODO: use a struct not pickle
        """
        self.index.Put(key, dumps((chunk.key, pos)), sync=sync)

    def new(self, cls, sync=False):
        """Create a new instance of 'cls'.
        """
        chunk = self.chunk
        head = chunk.head
        clssize = sizeof(cls)
        if head > chunk.size:
            raise TypeError('Type is too large %s for chunk %s.'
                            % (head, chunk.size))

        # not enough space in this chunk? Make a new one.
        if (head + clssize) > chunk.size:
            chunk.flush()
            chunk = self.new_chunk()
            head = chunk.head

        # advance the head pointer past the new object
        next_head = head + clssize
        chunk.head = next_head
        if sync:
            chunk.flush()
        return cls.from_chunk(chunk, head)

    def get(self, key, cls, default=None):
        """Fetch an instance of 'cls' with 'key'. """
        try:
            obj, pos = self._get_obj_pos(key)
        except KeyError:
            return default
        return cls.from_chunk(obj, pos)

    def put(self, obj, sync=True):
        """ Put an instance in the store. 

        If the object has a false key one will be generated.
        The object's map space will be marked as used.
        """
        if not obj.key:
            obj.key = str(uuid.uuid1())
        obj.used = True
        self._set_obj_pos(obj.key, obj.chunk, obj.pos, sync)
        if sync:
            obj.chunk.flush()

    def delete(self, obj, sync=True):
        """ Remove an instance from the index.

        The objects map space will be marked as unused.  Note this
        does not reclaim any store space, the objects space
        effectively becomes a "hole".
        """
        self.index.Delete(obj.key, sync)
        obj.used = False
        if sync:
            obj.chunk.flush()
