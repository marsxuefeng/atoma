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

package atoma.storage.mongo.command;

import com.mongodb.MongoException;
import java.util.Arrays;

/** <a href="https://www.mongodb.com/docs/manual/reference/error-codes/">MongoDB Error Code </a> */
public enum MongoErrorCode {
  UNKNOWN_ERROR(8, "UnknownError"),
  INTERNAL_ERROR(1, "InternalError"),
  BAD_VALUE(2, "BadValue"),
  HOST_UNREACHABLE(6, "HostUnreachable"),
  HOST_NOT_FOUND(7, "HostNotFound"),
  OVERFLOW(15, "Overflow"),
  INVALID_BSON(22, "InvalidBSON"),
  LOCK_TIMEOUT(22, "LockTimeout"),
  LOCK_BUSY(46, "LockBusy"),
  NETWORK_TIMEOUT(89, "NetworkTimeout"),
  LOCK_FAILED(107, "LockFailed"),
  WRITE_CONFLICT(112, "WriteConflict"),
  COMMAND_NOT_SUPPORTED(115, "CommandNotSupported"),
  NETWORK_INTERFACE_EXCEEDED_TIME_LIMIT(202, "NetworkInterfaceExceededTimeLimit"),
  NO_SUCH_TRANSACTION(251, "NoSuchTransaction"),
  EXCEEDED_TIME_LIMIT(262, "ExceededTimeLimit"),
  TOO_MANY_FILES_OPEN(264, "TooManyFilesOpen"),
  TRANSACTION_EXCEEDED_LIFETIME_LIMIT_SECONDS(290, "TransactionExceededLifetimeLimitSeconds"),
  DUPLICATE_KEY(11000, "DuplicateKey"),


  ;

  private final int code;
  private final String cause;

  MongoErrorCode(int code, String cause) {
    this.code = code;
    this.cause = cause;
  }

  static MongoErrorCode fromException(MongoException error) {
    return Arrays.stream(MongoErrorCode.values())
        .filter(t -> t.code == error.getCode())
        .findFirst()
        .orElse(MongoErrorCode.UNKNOWN_ERROR);
  }

  public int getCode() {
    return code;
  }

  public String getCause() {
    return cause;
  }
}