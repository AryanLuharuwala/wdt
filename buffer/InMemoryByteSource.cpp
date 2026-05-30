/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <glog/logging.h>
#include <wdt/buffer/InMemoryByteSource.h>

#include <algorithm>

namespace facebook {
namespace wdt {

InMemoryByteSource::InMemoryByteSource(SourceMetaData* metadata,
                                       const char* data, int64_t size,
                                       int64_t offset)
    : metadata_(metadata), data_(data), size_(size), offset_(offset) {
  transferStats_.setId(getIdentifier());
}

ErrorCode InMemoryByteSource::open(ThreadCtx* threadCtx) {
  bytesRead_ = 0;
  threadCtx_ = threadCtx;
  transferStats_.setLocalErrorCode(OK);
  return OK;
}

void InMemoryByteSource::advanceOffset(int64_t numBytes) {
  offset_ += numBytes;
  size_ -= numBytes;
}

char* InMemoryByteSource::read(int64_t& size) {
  size = 0;
  if (hasError() || finished()) {
    return nullptr;
  }
  // Hand out the largest slice that fits in the network buffer. The data is
  // caller-owned and stable for the lifetime of this source, so we can point
  // wdt directly at it without an intermediate copy.
  const int64_t bufferSize = threadCtx_->getBuffer()->getSize();
  const int64_t toRead = std::min<int64_t>(bufferSize, size_ - bytesRead_);
  char* const data = const_cast<char*>(data_ + offset_ + bytesRead_);
  bytesRead_ += toRead;
  size = toRead;
  WVLOG(1) << "InMemory read " << getIdentifier() << " size " << size
           << " offset " << offset_ << " bytesRead " << bytesRead_;
  return data;
}

void InMemoryByteSource::close() {
  threadCtx_ = nullptr;
}
}  // namespace wdt
}  // namespace facebook
