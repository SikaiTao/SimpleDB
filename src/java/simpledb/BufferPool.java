package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private ConcurrentHashMap<PageId, Page> bp;
    private int maxPagesSize;
    private LocksCatalog lc;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        maxPagesSize = numPages;
        bp = new ConcurrentHashMap<>();
        lc = new LocksCatalog();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

//    private class PagesBuffer extends LinkedHashMap<PageId, Page>{
//
//        private static final long serialVersionUID = 1L;
//        int maxPages;
//        private final Lock lock = new ReentrantLock();
//
//        private PagesBuffer(int maxPagesSize)
//        {
//            super(maxPagesSize, 1, true);
//            maxPages = maxPagesSize;
//        }
//
//        @Override
//        public Page get(Object key) {
//            try {
//                lock.lock();
//                return super.get(key);
//            } finally {
//                lock.unlock();
//            }
//        }
//
//        @Override
//        public Page put(PageId key, Page value) {
//            try {
//                lock.lock();
//                return super.put(key, value);
//            } finally {
//                lock.unlock();
//            }
//        }
//
//    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        long timeStamp = System.currentTimeMillis();

        while (!lc.lock(pid, tid, perm)){
            if (System.currentTimeMillis() - timeStamp > new Random().nextInt(500) + 2000)
                throw new TransactionAbortedException();
            try {
                Thread.sleep(new Random().nextInt(50));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Page tempPage = bp.get(pid);
        if(tempPage != null){
            return tempPage;
        }else{
            if (bp.size() == maxPagesSize) evictPage();
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page pageRead = file.readPage(pid);
            bp.put(pid, pageRead);
            return pageRead;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        lc.unlock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lc.isLocked(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        ArrayList<PageId> locks = lc.getLocksbyTid(tid);
        for (PageId pid : locks) {
            Page pg = bp.get(pid);
            if (pg != null) {
                if (commit) {
                    flushPage(pg.getId());
                    pg.setBeforeImage();

                } else if (pg.isDirty() != null){
                    discardPage(pid);
                }
            }
        }

        for (PageId pid : locks){
            lc.unlock(pid, tid);
        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile tarFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> insertPages = tarFile.insertTuple(tid, t);
        for (Page p : insertPages) {
            p.markDirty(true, tid);
            bp.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile tarFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> deletePages = tarFile.deleteTuple(tid, t);
        for (Page p : deletePages){
            p.markDirty(true, tid);
            bp.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        List<PageId> pids = new ArrayList<>(bp.keySet());
        for (PageId pid : pids) {
            flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        bp.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page targetPage = bp.get(pid);
        if (targetPage != null){
            TransactionId dirtyTid = targetPage.isDirty();
            if(dirtyTid != null){
                Database.getLogFile().logWrite(dirtyTid, targetPage.getBeforeImage(), targetPage);
                Database.getLogFile().force();

                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(targetPage);
                targetPage.markDirty(false, dirtyTid);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Page p : bp.values()) {
            if (p.isDirty() == tid) flushPage(p.getId());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        //NO-STEAL strategy
        PageId target = null;

        for (Map.Entry<PageId, Page> pair : bp.entrySet()){
            if (pair.getValue().isDirty() == null){
                target = pair.getKey();
                break;
            }
        }

        if (target == null) throw new DbException("All pages are dirty in buffer pool");

        discardPage(target);



        //STEAL strategy
//        PageId oldest =  new ArrayList<>(bp.keySet()).get(0);
//        try {
//            flushPage(oldest);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        discardPage(oldest);
    }

}
