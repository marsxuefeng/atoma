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

package atoma.api.coordination.command;

import java.util.concurrent.TimeUnit;

/**
 * A container for all commands and result types related to distributed {@link
 * atoma.api.synchronizer.CyclicBarrier} operations. This class encapsulates the different actions
 * that can be performed on a barrier resource.
 */
public final class CyclicBarrierCommand {

  private CyclicBarrierCommand() {}

  // --- Result Objects ---

  /**
   * Represents the result of an {@link Await} command.
   *
   * @param passed {@code true} if this awaiting party was the last one required to trip the
   *     barrier, {@code false} otherwise.
   * @param broken {@code true} if the barrier was broken either before or as a result of this
   *     await.
   * @param waited {@code true} if the barrier was waited successfully either before or as a result
   *     of this await.
   * @param generation The current generation of the barrier. A generation changes when the barrier
   *     is tripped or reset.
   */
  public record AwaitResult(boolean passed, boolean broken, boolean waited, long generation) {}

  /**
   * Represents the state of the barrier, returned by a {@link GetState} command.
   *
   * @param parties The number of parties required to trip this barrier.
   * @param numberWaiting The number of parties currently waiting at the barrier.
   * @param isBroken {@code true} if the barrier is in a broken state.
   * @param generation The current generation of the barrier. A generation changes when the barrier
   *     is tripped or reset.
   */
  public record GetStateResult(int parties, int numberWaiting, boolean isBroken, long generation) {}

  // --- Commands ---

  /**
   * Command for a party to await on the barrier.
   *
   * @param participantId the id of participant.
   * @param leaseId the lease of participant.
   * @param parties The number of parties required to trip the barrier. This is used to initialize
   *     the barrier on the first await.
   * @param generation The current generation of the barrier. A generation changes when the barrier
   *     is tripped or reset.
   * @param timeout maximum wait time.
   * @param timeUnit time unit
   */
  public record Await(
      String participantId,
      String leaseId,
      int parties,
      long generation,
      long timeout,
      TimeUnit timeUnit)
      implements Command<AwaitResult> {}

  /**
   * Command to break the barrier to its broken state.
   *
   * @param generation The current generation of the barrier. A generation changes when the barrier
   *     * is tripped or reset.
   */
  public record Break(long generation) implements Command<Void> {}

  /**
   * Command to reset the barrier to its initial state. This is useful for re-using a barrier after
   * it has been tripped or broken.
   */
  public record Reset(int parties) implements Command<GetStateResult> {}

  /**
   * Command to retrieve the current state of the barrier. This is primarily used for monitoring and
   * debugging.
   *
   * @param parties The number of parties required to trip this barrier.
   */
  public record GetState(int parties) implements Command<GetStateResult> {}
}