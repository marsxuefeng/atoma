/*
 * Copyright 2025 XueFeng Ma
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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