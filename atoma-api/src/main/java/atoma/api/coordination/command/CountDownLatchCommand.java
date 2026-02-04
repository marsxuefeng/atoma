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

/**
 * A container for all commands and result types related to distributed {@link
 * atoma.api.synchronizer.CountDownLatch} operations. This class encapsulates the different actions
 * that can be performed on a latch resource.
 */
public final class CountDownLatchCommand {

  private CountDownLatchCommand() {}

  // --- Result Objects ---

  /**
   * Represents the result of a {@link GetCount} command, containing the current count of the latch.
   *
   * @param version The current version of the latch.
   * @param count The current count of the latch. If it is negative, it means it does not exist
   */
  public record GetCountResult(int count, long version) {}

  // --- Commands ---

  /**
   * Command to initialize a distributed latch with a specific count. This operation is typically
   * conditional, setting the count only if the latch resource does not already exist.
   *
   * @param count The initial count for the latch.
   */
  public record Initialize(int count) implements Command<Void> {}

  /**
   * Command to decrement the latch count by one. If the count reaches zero, this may trigger the
   * release of waiting parties.
   */
  public record CountDown(int count) implements Command<Void> {}

  /** Command to retrieve the current count of the latch. */
  public record GetCount(int count) implements Command<GetCountResult> {}

  /**
   * Command to permanently delete the latch resource from the backend storage. This is used for
   * explicit resource cleanup.
   */
  public record Destroy() implements Command<Void> {}
}