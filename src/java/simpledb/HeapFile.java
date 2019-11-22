package simpledb;

import java.io.*;
import java.util.*;
import java.lang.Math;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

		private File f;
		private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
				this.f = f;
				this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
				return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
				return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
				return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgSize = Database.getBufferPool().getPageSize();
				byte[] data = new byte[pgSize];
				int offset = pgSize * pid.getPageNumber();
				RandomAccessFile raFile = null;
				HeapPage pg = null;
				
				try {
					raFile = new RandomAccessFile(f, "r");
					raFile.seek(offset);
					int readBytes = raFile.read(data);
					assert(readBytes == pgSize);
					raFile.close();
					pg = new HeapPage((HeapPageId)pid, data);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					try {
						raFile.close();
					} catch(Exception e){
						e.printStackTrace();
					}
				}

				return pg;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int pgSize = Database.getBufferPool().getPageSize();
        return (int)Math.ceil((double)f.length() / pgSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

		private class HeapFileIterator implements DbFileIterator {
				private Iterator<Tuple> pageIterator;
				private int pageNo;
				private TransactionId tid;

				public HeapFileIterator(TransactionId tid) {
						this.tid = tid;
				}

				public void open() throws DbException, TransactionAbortedException {
						pageNo = 0;
						HeapPageId id = new HeapPageId(HeapFile.this.getId(), pageNo);
						HeapPage pg =
							(HeapPage) Database.getBufferPool().getPage(tid, id, Permissions.READ_WRITE);
						pageIterator = pg.iterator();
				}

				public boolean hasNext() {
						return pageNo < HeapFile.this.numPages();
				}

				public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
						Tuple t = pageIterator.next();

						if (!pageIterator.hasNext()) {
							pageNo++;
							if (pageNo != HeapFile.this.numPages()) {
									HeapPageId id = new HeapPageId(HeapFile.this.getId(), pageNo);
									HeapPage pg =
										(HeapPage) Database.getBufferPool().getPage(tid, id, Permissions.READ_WRITE);
									pageIterator = pg.iterator();
							}
						}

						return t;
				}

				public void rewind() throws DbException, TransactionAbortedException {
						throw new DbException("rewind not supported");
				}

				public void close() {
						// do nothing
				}
		}

    // see DbFile.java for javadocs
		// -- TC -- this iterator will read page from the bufferpool
		// if the page is not in bufferpool, the bufferpool will load
		// the page from the disk, which uses readPage() above.
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
				return new HeapFileIterator(tid);
    }
}

