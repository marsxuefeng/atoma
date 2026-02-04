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

import atoma.api.coordination.Resource;

import java.util.Optional;

/**
 * Provides the context required for a {@link CommandHandler} to execute a command.
 *
 * <p>This context supplies essential information about the resource being acted upon,
 * such as its ID and its state prior to the command's execution.
 */
public interface CommandHandlerContext {

  /**
   * Returns the identifier of the resource being processed.
   *
   * @return The unique key of the resource.
   */
  String getResourceId();

  /**
   * Returns the state of the resource as it was before the command started executing.
   *
   * <p>This allows the handler to inspect the resource's previous state to make decisions.
   *
   * @return An {@link Optional} containing the resource's state, or an empty optional
   *         if the resource did not exist before this operation.
   */
  Optional<Resource> getCurrentResource();
}