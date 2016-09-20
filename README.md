Clownshoes is an experiment in a very simple document (which is to say "fistful of bytes") database.  Its underlying storage schema is essentially a mmap'd file containing a linked list of byte arrays.  The name is a reference to a different more prominent document database with a [similar approach](http://nyeggen.com/post/2013-11-25-clownshoes-an-enterprise-grade-etc-etc-document-store/).

Instead of journaling, we just maintain a shared mmap'd buffer.  If you wish to have a guaranteed durable write, you must snapshot the entire DB after the write.

Indexing is entirely in memory via hash tables, which must be snapshotted along with the main DB if you wish to reuse them instead of creating them anew on each startup.  Queries which wish to use indexing must specify the index, and support equality lookup only.

Because of the limited intended use case, it's unlikely that journaling will be implemented.  The implication of  this is that you really shouldn't use Clownshoes for "production" data.

If you are looking for a more "hardcore" embeddable document database, [tiedot](https://github.com/HouzuoGuo/tiedot) may be more to your liking.  Or, if you are willing to use some native code, use [SQLite](https://github.com/mattn/go-sqlite3), which rocks.
