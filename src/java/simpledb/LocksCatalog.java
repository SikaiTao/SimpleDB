package simpledb;


// page-level locks catalog
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class LocksCatalog {
    private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> locksOnPage;

    public LocksCatalog(){
        locksOnPage = new ConcurrentHashMap<>();
    }

    public synchronized boolean lock(PageId pid, TransactionId tid, Permissions perm) {
        ConcurrentHashMap<TransactionId, PageLock> ls = locksOnPage.get(pid);

        if (ls == null) {
            PageLock l = new PageLock(tid, perm);
            ConcurrentHashMap<TransactionId, PageLock> locks = new ConcurrentHashMap<>();
            locks.put(tid, l);
            locksOnPage.put(pid, locks);
            return true;
        }

        PageLock l = ls.get(tid);
        if (l != null){
            if (l.perm == perm) return true;

            if (l.perm == Permissions.READ_ONLY && perm == Permissions.READ_WRITE){
                if (ls.size() != 1) return false;
                l.perm = Permissions.READ_WRITE;
                return true;
            }

            if (l.perm == Permissions.READ_WRITE && perm == Permissions.READ_ONLY){
                return true;
            }
        }else {
            if (ls.values().iterator().next().perm == Permissions.READ_WRITE || perm == Permissions.READ_WRITE) return false;

            PageLock pl = new PageLock(tid, perm);
            ls.put(tid, pl);
            locksOnPage.put(pid, ls);
            return true;
        }
        System.out.println("This should not happen");
        return true;
    }


    public synchronized void unlock(PageId pid, TransactionId tid){
        ConcurrentHashMap<TransactionId, PageLock> ls = locksOnPage.get(pid);
        ls.remove(tid);
        if (ls.size() == 0) locksOnPage.remove(pid);
    }


    public synchronized boolean isLocked(PageId pid, TransactionId tid){
        if (locksOnPage.get(pid) == null) return false;
        else return locksOnPage.get(pid).get(tid) !=null;
    }

    public synchronized ArrayList<PageId> getLocksbyTid(TransactionId tid){
        ArrayList<PageId> lst = new ArrayList<>();
        for(PageId pid : locksOnPage.keySet()) {
            if (locksOnPage.get(pid).get(tid) != null)
                lst.add(pid);
        }
        return lst;
    }


    private class PageLock{
        TransactionId tid;
        Permissions perm;

        PageLock(TransactionId tid, Permissions perm){
            this.tid = tid;
            this.perm = perm;
        }
    }
}
