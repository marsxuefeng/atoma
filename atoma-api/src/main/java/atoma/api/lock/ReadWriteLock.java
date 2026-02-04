package atoma.api.lock;

import atoma.api.Leasable;

/**
 * A {@code ReadWriteLock} maintains a pair of associated {@link Lock locks}, one for read-only
 * operations and one for writing. The {@link #readLock() read lock} may be held simultaneously by
 * multiple reader threads, so long as there are no writers. The {@link #writeLock() write lock} is

 * exclusive.
 *
 * <p>A {@code ReadWriteLock} is {@link Leasable}, meaning it can be leased for a specific duration.
 * When the lease expires, both the read and write locks are automatically released.
 *
 * <p>Implementations of this class are expected to be thread-safe.
 */
public abstract class ReadWriteLock extends Leasable {

  /**
   * Returns the lock used for reading.
   *
   * @return the lock used for reading
   */
  public abstract Lock readLock();

  /**
   * Returns the lock used for writing.
   *
   * @return the lock used for writing
   */
  public abstract Lock writeLock();
}
