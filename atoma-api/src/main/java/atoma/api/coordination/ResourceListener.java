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

package atoma.api.coordination;

/**
 * A functional callback interface for receiving state change events of a {@link Resource}.
 *
 * <p>Implementations of this interface are typically registered via {@link
 * CoordinationStore#subscribe(Class, String, ResourceListener)} to listen for updates, creations,
 * or deletions of specific distributed resources. Being a functional interface, it can be
 * implemented concisely using lambda expressions.
 */
@FunctionalInterface
public interface ResourceListener {

  /**
   * Called when a state change event occurs for the subscribed resource.
   *
   * @param event The {@link ResourceChangeEvent} object containing detailed information about the
   *     event, including the type of change and the new/old resource state.
   */
  void onEvent(ResourceChangeEvent event);
}