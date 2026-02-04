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
 * A command handler that executes a specific type of Command.
 *
 * @param <C> The type of Command this handler can process.
 * @param <R> The type of Result this command produces.
 */
@FunctionalInterface
public interface CommandHandler<C extends Command<R>, R> {

  /**
   * Handles a command in the given context.
   *
   * @param command The command to handle.
   * @param context The context providing resources for execution, like the database session.
   * @return A command-specific result object.
   */
  R execute(C command, CommandHandlerContext context);
}
