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

import java.util.concurrent.atomic.AtomicBoolean;

/** Resourceful of class. */
public abstract class Resourceful implements AutoCloseable {

  protected final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * @return The resource unique-id
   */
  public abstract String getResourceId();

  /**
   * @return The current resource is closed.
   */
  public boolean isClosed() {
    return closed.get();
  }
}