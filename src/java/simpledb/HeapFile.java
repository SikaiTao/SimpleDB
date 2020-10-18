package simpledb;

import java.io.*;
import java.util.*;

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
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        byte[] container = new byte[BufferPool.getPageSize()];
        Page targetPage = null;
        try{
            InputStream is = new FileInputStream(f);
            int offset = pid.getPageNumber() * BufferPool.getPageSize();
            if(offset > 0)
                is.skip(offset);

            is.read(container);
            targetPage = new HeapPage((HeapPageId)pid,container);
            is.close();

        }catch (IOException e){
            e.printStackTrace();
        }

        return targetPage;
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
        return (int)(f.length() / BufferPool.getPageSize());
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

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new AbstractDbFileIterator() {
            Iterator<Tuple> currentPageI;
            int i;
            boolean open = false;

            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if(!open)
                    return null;
                if (currentPageI.hasNext())
                    return currentPageI.next();
                else {
                    if(i < numPages()){
                        currentPageI = ((HeapPage)(Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i++), null))).iterator();
                        return this.readNext();
                    }else return null;
                }

            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (numPages() <= 0)
                    throw new DbException("Heap file is empty or invalid");
                else {
                    open = true;
                    rewind();
                }

            }

            @Override
            public void close() {
                super.close();
                currentPageI = null;
                open = false;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                currentPageI = ((HeapPage)(Database.getBufferPool().getPage(tid, new HeapPageId(getId(), 0), null))).iterator();
                i = 1;
            }
        };
    }

}

