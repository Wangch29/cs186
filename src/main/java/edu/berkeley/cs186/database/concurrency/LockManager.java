package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 * <p>
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multi-granularity is handled by LockContext instead.
 * <p>
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 * <p>
 * This does mean that in the case of:
 * queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            for (Lock lock : locks) {
                if (lock.transactionNum == except) {  // Ignore except.
                    continue;
                }
                if (!LockType.compatible(lockType, lock.lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Overloads checkCompatible with default except -1L.
         */
        public boolean checkCompatible(LockType lockType) {
            return checkCompatible(lockType, -1L);
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            for (Lock l : this.locks) {
                if (Objects.equals(l.transactionNum, lock.transactionNum)) {
                    l.lockType = lock.lockType;  // Update lock type.
                    return;
                }
            }
            this.locks.add(lock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            for (Lock l : this.locks) {
                if (l == lock) {
                    this.locks.remove(l);
                    this.processQueue();
                    return;
                }
            }
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            if (addFront) {
                this.waitingQueue.addFirst(request);
            }
            this.waitingQueue.addLast(request);
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> iter = this.waitingQueue.iterator();
            while (iter.hasNext()) {
                LockRequest request = iter.next();

                if (request.lock.lockType == LockType.NL) {
                    iter.remove();
                    continue;
                }

                // Check if the request is compatible.
                if (this.checkCompatible(request.lock.lockType, request.transaction.getTransNum())) {
                    iter.remove();
                    this.grantOrUpdateLock(request.lock);
                    List<Lock> lockList = getTransactionLocksEntry(request.transaction.getTransNum());

                    // If lock already in lockList, update lockType.
                    boolean update = false;
                    for (Lock l : lockList) {
                        if (Objects.equals(l.transactionNum, request.lock.transactionNum)) {
                            l.lockType = request.lock.lockType;
                            update = true;
                        }
                    }
                    // Add lock to lockList.
                    if (!update) {
                        lockList.add(request.lock);
                    }
                    request.transaction.unblock();
                } else {  // Incompatible, just return.
                    return;
                }
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        this.resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    private List<Lock> getTransactionLocksEntry(Long transactionNumber) {
        this.transactionLocks.putIfAbsent(transactionNumber, new ArrayList<>());
        return transactionLocks.get(transactionNumber);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     * <p>
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     * <p>
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     * <p>
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     *                                       by `transaction` and isn't being released
     * @throws NoLockHeldException           if `transaction` doesn't hold a lock on one
     *                                       or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        boolean grantLock = false;
        synchronized (this) {
            // Check if already hold the lock
            List<Lock> lockList = this.getTransactionLocksEntry(transaction.getTransNum());
            for (Lock lock : lockList) {
                if (lock.name == name && lock.lockType == lockType) {
                    throw new DuplicateLockRequestException("Already hold a lock on " + name);
                }
            }

            if (!this.resourceEntries.containsKey(name)) {
                this.resourceEntries.put(name, new ResourceEntry());
            }
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            ResourceEntry resourceEntry = this.getResourceEntry(name);

            // Grant.
            if (resourceEntry.checkCompatible(lockType)) {
                grantLock = true;
                resourceEntry.grantOrUpdateLock(lock);
                this.getTransactionLocksEntry(transaction.getTransNum()).add(lock);
            } else {  // Cannot grant.
                shouldBlock = true;
                resourceEntry.addToQueue(new LockRequest(transaction, lock), true);
            }

            // Check if name is in releaseNames.
            if (shouldBlock) {
                for (ResourceName rn : releaseNames) {
                    // Search if name is in releaseNames.
                    if (rn == name) {
                        boolean inRelease = false;
                        // Release previous lock.
                        Iterator<Lock> iter = lockList.iterator();
                        Lock l = null;
                        // Search if name is in lockList.
                        while (iter.hasNext()) {
                            l = iter.next();
                            if (l.name == rn) {
                                inRelease = true;
                                break;
                            }
                        }
                        // If rn in lockList, release old locks and grant new lock.
                        if (inRelease) {
                            iter.remove();
                            this.resourceEntries.get(rn).releaseLock(l);
                            ResourceEntry re = this.getResourceEntry(name);
                            re.grantOrUpdateLock(lock);
                            shouldBlock = false;
                            break;
                        }
                    }
                }
            }

            // Release resourceNames, if succeed to grant the lock.
            if (grantLock) {
                for (ResourceName rn : releaseNames) {
                    if (rn == name) {
                        continue;
                    }

                    boolean absent = true;

                    Iterator<Lock> iter = lockList.iterator();
                    while (iter.hasNext()) {
                        Lock l = iter.next();
                        if (l.name == rn) {
                            absent = false;
                            iter.remove();
                            this.resourceEntries.get(rn).releaseLock(l);
                        }
                    }

                    if (absent) {
                        throw new NoLockHeldException("No such a holding lock on " + rn.toString());
                    }
                }
            }
            if (shouldBlock) {
                transaction.prepareBlock();
            }
        }

        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     * <p>
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     *                                       `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            // Check if already hold the lock
            List<Lock> lockList = this.getTransactionLocksEntry(transaction.getTransNum());
            for (Lock lock : lockList) {
                if (lock.name == name) {
                    throw new DuplicateLockRequestException("Already hold a lock on " + name);
                }
            }

            if (!this.resourceEntries.containsKey(name)) {
                this.resourceEntries.put(name, new ResourceEntry());
            }
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            ResourceEntry resourceEntry = this.getResourceEntry(name);

            if (resourceEntry.checkCompatible(lockType) && resourceEntry.waitingQueue.size() == 0) {
                resourceEntry.grantOrUpdateLock(lock);
                this.getTransactionLocksEntry(transaction.getTransNum()).add(lock);
            } else {  // Incompatible
                shouldBlock = true;
                resourceEntry.addToQueue(new LockRequest(transaction, lock), false);
            }

            if (shouldBlock) {
                transaction.prepareBlock();
            }
        }

        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     * <p>
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        synchronized (this) {
            List<Lock> lockList = this.getTransactionLocksEntry(transaction.getTransNum());
            Iterator<Lock> it = lockList.iterator();
            while (it.hasNext()) {
                Lock lock = it.next();
                if (lock.name == name) {
                    it.remove();
                    this.resourceEntries.get(name).releaseLock(lock);
                    return;
                }
            }
            throw new NoLockHeldException("No lock on " + name);
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if it's a valid substitution).
     * <p>
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     * <p>
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     *                                       `newLockType` lock on `name`
     * @throws NoLockHeldException           if `transaction` has no lock on `name`
     * @throws InvalidLockException          if the requested lock type is not a
     *                                       promotion. A promotion from lock type A to lock type B is valid if and
     *                                       only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            List<Lock> lockList = this.getTransactionLocksEntry(transaction.getTransNum());
            boolean exist = false;

            for (Lock lock : lockList) {
                if (lock.name == name) {
                    exist = true;
                    if (lock.lockType == newLockType) {  // Already hold the lock type "newLockType".
                        throw new DuplicateLockRequestException("Already hold a lock on " + name);
                    }
                    // Check substitutability.
                    if (!LockType.substitutable(newLockType, lock.lockType)) {
                        throw new InvalidLockException("Cannot promote from " + lock.lockType + " to " + newLockType);
                    }
                    // Check compatibility, except transaction itself.
                    if (!this.resourceEntries.get(name).checkCompatible(newLockType, transaction.getTransNum())) {
                        shouldBlock = true;
                        this.resourceEntries.get(name).addToQueue(
                                new LockRequest(transaction, new Lock(name, newLockType, transaction.getTransNum())),
                                true
                        );
                        break;
                    }
                    lock.lockType = newLockType;
                    this.resourceEntries.get(name).grantOrUpdateLock(new Lock(name, newLockType, transaction.getTransNum()));
                    return;
                }
            }
            if (!exist) {
                throw new NoLockHeldException("No lock on " + name);
            }

            if (shouldBlock) {
                transaction.prepareBlock();
            }
        }

        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
