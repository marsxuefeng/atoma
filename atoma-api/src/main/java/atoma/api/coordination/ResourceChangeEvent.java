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

import java.util.Optional;

public final class ResourceChangeEvent {

  public ResourceChangeEvent(EventType type, String resourceKey, Resource newNode, Resource oldNode) {
    this.type = type;
    this.resourceKey = resourceKey;
    this.newNode = Optional.ofNullable(newNode);
    this.oldNode = Optional.ofNullable(oldNode);
  }

  public enum EventType {
    /** 节点被创建。getNewNode() 将返回被创建的节点。 */
    CREATED,
    /** 节点被更新。getNewNode() 和 getOldNode() 分别返回更新后和更新前的节点状态。 */
    UPDATED,
    /** 节点被删除。getOldNode() 将返回被删除前的最后状态。 */
    DELETED
  }

  private final EventType type;
  private final String resourceKey;

  /** 删除场景下，此字段为NULL */
  private final Optional<Resource> newNode;

  /** 创建场景下，此字段为空 */
  private final Optional<Resource> oldNode;

  public EventType getType() {
    return type;
  }

  public String getResourceKey() {
    return resourceKey;
  }

  public Optional<Resource> getNewNode() {
    return newNode;
  }

  public Optional<Resource> getOldNode() {
    return oldNode;
  }
}