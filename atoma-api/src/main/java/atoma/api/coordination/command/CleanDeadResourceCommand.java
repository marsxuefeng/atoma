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
 * A container for commands related to cleaning up dead or expired resources
 * within the coordination service.
 */
public final class CleanDeadResourceCommand {

  /**
   * Represents a command to trigger the cleanup of dead resources.
   *
   * <p>This command signals the {@link atoma.api.coordination.CoordinationStore}
   * to identify and remove any resources that are no longer valid, such as
   * locks held by expired leases. It does not return any value upon completion.
   * @param leaseId The lease ID of the client.
   */
  public record Clean(String leaseId) implements Command<Void> {}
}