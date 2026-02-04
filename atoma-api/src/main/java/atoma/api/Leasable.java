package atoma.api;

/**
 * Represents a resource that can be leased. A lease is a mechanism that grants temporary ownership
 * of a resource to a client.
 *
 * <p>Leasable resources are designed to be fault-tolerant. If a client acquires a lease and then
 * fails, the lease will eventually expire, allowing other clients to acquire the resource. This
 * prevents the resource from being locked indefinitely.
 *
 * <p>Clients that hold a lease are responsible for renewing it before it expires to maintain
 * ownership.
 *
 * @see Lease
 */
public abstract class Leasable extends Resourceful {

  /**
   * Returns the unique identifier of the current lease on this resource.
   *
   * <p>If the resource is not currently leased, the behavior may vary by implementation, but it
   * will typically return {@code null}.
   *
   * @return the lease ID, or {@code null} if the resource is not currently under a lease
   */
  public abstract String getLeaseId();
}
