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

package atoma.core;

final class ThreadUtils {

  public static String getCurrentThreadId() {
    return String.format("%s-%d", Thread.currentThread().getName(), Thread.currentThread().getId());
  }

  public static String getCurrentHolderId(String leaseId) {
    return String.format("%s-%s", leaseId, getCurrentThreadId());
  }
}