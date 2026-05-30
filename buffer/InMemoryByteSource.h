/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <wdt/ByteSource.h>
#include <wdt/util/CommonImpl.h>

#include <string>

namespace facebook {
namespace wdt {

/**
 * ByteSource that streams data from a contiguous, caller-owned in-memory
 * buffer instead of a file. This lets the wdt library send a buffer without
 * ever touching the filesystem (@see wdt/buffer/WdtBuffer.h).
 *
 * The buffer must outlive the InMemoryByteSource. No copy of the buffer is
 * made; read() hands wdt slices of the caller's memory directly.
 */
class InMemoryByteSource : public ByteSource {
 public:
  /**
   * Create a new InMemoryByteSource over a caller-owned buffer.
   *
   * @param metadata    shared source data. identifier is taken from relPath
   * @param data        pointer to the start of the buffer (not owned)
   * @param size        number of bytes to stream from data
   * @param offset      offset into the buffer at which this block starts
   */
  InMemoryByteSource(SourceMetaData* metadata, const char* data, int64_t size,
                     int64_t offset);

  ~InMemoryByteSource() override {
    this->close();
  }

  /// @return identifier (relative path) for the source
  const std::string& getIdentifier() const override {
    return metadata_->relPath;
  }

  /// @return number of bytes in this block
  int64_t getSize() const override {
    return size_;
  }

  /// @return offset from which to start reading
  int64_t getOffset() const override {
    return offset_;
  }

  /// @see ByteSource.h
  const SourceMetaData& getMetaData() const override {
    return *metadata_;
  }

  /// @return true iff all bytes have been handed out
  bool finished() const override {
    return bytesRead_ == size_ && !hasError();
  }

  /// @return true iff there was an error reading. In-memory reads can not fail
  bool hasError() const override {
    return false;
  }

  /// @see ByteSource.h
  char* read(int64_t& size) override;

  /// @see ByteSource.h
  void advanceOffset(int64_t numBytes) override;

  /// @see ByteSource.h
  ErrorCode open(ThreadCtx* threadCtx) override;

  /// close the source for reading
  void close() override;

  /// @see ByteSource.h
  TransferStats& getTransferStats() override {
    return transferStats_;
  }

  /// @param stats    Stats to be added
  void addTransferStats(const TransferStats& stats) override {
    transferStats_ += stats;
  }

 private:
  /// thread context, set by open()
  ThreadCtx* threadCtx_{nullptr};

  /// shared source metadata
  SourceMetaData* metadata_;

  /// start of the caller's buffer (not owned)
  const char* data_;

  /// number of bytes in this block
  int64_t size_;

  /// block offset into the buffer
  int64_t offset_;

  /// number of bytes handed out so far
  int64_t bytesRead_{0};

  /// transfer stats
  TransferStats transferStats_;
};
}  // namespace wdt
}  // namespace facebook
