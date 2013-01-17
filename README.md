= structchunk

Structured data store to mmap'ed chunk files.

This is a very simple key, object data store, that maps
ctypes.Structure objects onto a memory mapped sparse chunk file.  The
index of keys and the offset into the mapped chunk files is stored in a
leveldb database.

Structure objects contain only data accessor methods into a given
memory buffer. No copying is done in Python from the file to memory,
all handling of the memory buffers backing the Structure objects is
done by the kernel's virtual memory manager.

== Advantages

  - LevelDB index is fast and lightweight, only the key and the offset
    into a chunked file is stored.

  - Objects can be accessed from on-disk data without explicitly
    serializing or unserializing any of the data in process memory
    space.  The VMM takes care of all disk access.

  - All existing 'ctypes' types can be used as-is, no special type
    system exists, a single Object superclass is provided for defining
    new Chunk stored structures, but otherwise is exactly like a
    ctypes.Structure.

  - Objects are equivalent to c structs unrelated to any Python
    structures, making the file format portable.

  - Sparse file support means only initialized structure elements get
    written to disk.  Large arrays can be allocated but only
    initialized elements take up any disk space.

  - More data can be referenced in mapped files than physical memory
    can hold, the OS takes care of loading and unloading virtual
    memory backing objects to fit into available memory automatically.

  - Objects are simply offset pointers into a chunk file, and have a
    comparatively small memory footprint compared to the data they can
    reference on disk.
    
== Disadvantages

  - If the index is lost, the chunk files are meaningless.

  - If the OS doesn't handle sparse files (OSX) chunk files are fully
    sized.

  - The OS process virtual memory space limits apply to mmap()ed
    files, 32-bit linux is limited to 2 GBs of on disk data.  64-bit
    systems can address up to 128 TB.

  - No "type" information is stored on objects in the chunk file, you
    must know the type head of time before you load it.  If you load a
    buffer into the wrong type, weird stuff will happen!

== Implementation

ctypes.Structure subclasses have a from_buffer() method that creates a
new instance of that type backed by the memory region in the buffer.
By using an mmap.mmap object that is mapped to a chunk file as the
buffer, the objects are essentially accessors to data in process
virtual memory that is directly on disk.

Every chunk file contains a header that specifies its size, and the
next available "free" spot in the file that has not been allocated.
When a new object is created, it is mapped to the free location and
the head is advanced to the end of the new object.  If there is
issuficient space in the chunk to hold the new object, a new chunk is
created.

