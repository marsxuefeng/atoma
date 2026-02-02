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
   * @param generation The current generation of the barrier. A generation changes when the barrier
   *     is tripped or reset.
   */
  public record AwaitResult(boolean passed, boolean broken, long generation) {}

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
   * @param parties The number of parties required to trip the barrier. This is used to initialize
   *     the barrier on the first await.
   */
  public record Await(int parties, long timeout, TimeUnit timeUnit)
      implements Command<AwaitResult> {}

  /**
   * Command to reset the barrier to its initial state. This is useful for re-using a barrier after
   * it has been tripped or broken.
   */
  public record Reset() implements Command<Void> {}

  /**
   * Command to retrieve the current state of the barrier. This is primarily used for monitoring and
   * debugging.
   *
   * @param parties The number of parties required to trip this barrier.
   */
  public record GetState(int parties) implements Command<GetStateResult> {}
}
