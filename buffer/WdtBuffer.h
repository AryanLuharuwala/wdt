/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <wdt/ErrorCodes.h>
#include <wdt/WdtTransferRequest.h>

#include <cstddef>
#include <memory>
#include <string>

namespace facebook {
namespace wdt {

/**
 * WdtBuffer is a thin, header-forward library wrapper that turns wdt from a
 * process/file oriented tool into a simple in-memory buffer transport.
 *
 * It is built entirely on top of the in-process wdt library API
 * (facebook::wdt::Sender / Receiver / WdtTransferRequest): no `wdt` binary is
 * spawned and nothing is ever persisted to durable storage. A caller hands a
 * contiguous buffer to BufferSender::send() and the matching bytes are
 * delivered into a buffer on the BufferReceiver side.
 *
 * Send side: BufferSender::send() copies the caller's buffer into an anonymous,
 * in-memory file created with memfd_create(2) and hands that fd to wdt's
 * in-process Sender. wdt then transmits it over its existing fd read path
 * (FileByteSource reading from the memfd); no on-disk file is created and no
 * subprocess is spawned. (Note: this does NOT use InMemoryByteSource on the
 * live send path -- see the portability/status note below.)
 *
 * Receive side reuses wdt's existing Receiver, which writes blocks through a
 * FileWriter. To deliver into a caller buffer without persisting to durable
 * storage, the receiver stages the incoming bytes to a RAM-backed scratch file
 * ($scratchDir/wdt_buffer) in a tmpfs scratch directory (under /dev/shm), then
 * reads that file back into the caller's buffer and unlinks it once the
 * transfer completes. The staged data is a real filesystem file going through
 * wdt's FileWriter machinery, but because it lives on tmpfs it is RAM-backed
 * and never touches a physical disk. The on-disk wdt receiver internals are
 * left untouched so this stays a cheap, non-invasive library layer.
 *
 * InMemoryByteSource (@see wdt/buffer/InMemoryByteSource.h) is provided and
 * unit-tested as a ByteSource that streams directly from a caller-owned buffer,
 * but it is NOT yet wired into the live send path above. Wiring it in would
 * require making the Sender accept a custom SourceQueue, which is deliberately
 * deferred; until then the memfd approach is used instead.
 *
 * Portability: this implementation is currently Linux-only. The send path needs
 * memfd_create(2) (Linux >= 3.17, glibc >= 2.27) and the receive path requires
 * a tmpfs mounted at /dev/shm. macOS (which upstream wdt CI targets) is not yet
 * supported; a portable fallback is future work.
 *
 * Example (loopback):
 * @code
 *   BufferReceiver receiver;
 *   receiver.start();                       // picks ports, starts listening
 *   const WdtTransferRequest& conn = receiver.getConnectionRequest();
 *
 *   const char buf[] = "hello wdt";
 *   BufferSender::send(conn, buf, sizeof(buf));
 *
 *   std::string out;
 *   receiver.finish(out);                   // out now holds the sent bytes
 * @endcode
 */

/// Sends a single contiguous in-memory buffer to a listening BufferReceiver.
class BufferSender {
 public:
  /**
   * Send a buffer to a receiver described by a wdt connection url.
   *
   * @param url     wdt connection url obtained from the receiver
   *                (@see BufferReceiver::getConnectionUrl)
   * @param buf     pointer to the bytes to send (not modified, not owned)
   * @param len     number of bytes to send
   *
   * @return        OK on success, otherwise the transfer error code
   */
  static ErrorCode send(const std::string& url, const void* buf, size_t len);

  /**
   * Send a buffer to a receiver described by a wdt connection request.
   *
   * @param connectionRequest   connection request obtained from the receiver
   *                            (@see BufferReceiver::getConnectionRequest)
   * @param buf                 pointer to the bytes to send
   * @param len                 number of bytes to send
   *
   * @return        OK on success, otherwise the transfer error code
   */
  static ErrorCode send(const WdtTransferRequest& connectionRequest,
                        const void* buf, size_t len);
};

/// Receives a single buffer sent by a BufferSender into caller memory.
class BufferReceiver {
 public:
  BufferReceiver();
  ~BufferReceiver();

  // non-copyable: owns an underlying wdt Receiver and scratch directory
  BufferReceiver(const BufferReceiver&) = delete;
  BufferReceiver& operator=(const BufferReceiver&) = delete;

  /**
   * Start listening for an incoming buffer. Ports are auto-assigned. After
   * this returns OK, getConnectionUrl() / getConnectionRequest() describe how
   * a sender can reach this receiver.
   *
   * @param numPorts    number of ports/connections to listen on
   *
   * @return            OK if the receiver started successfully
   */
  ErrorCode start(int numPorts = 1);

  /// @return   wdt connection url to hand to a BufferSender. Empty if not
  ///           started.
  std::string getConnectionUrl() const;

  /// @return   wdt connection request to hand to a BufferSender
  const WdtTransferRequest& getConnectionRequest() const;

  /**
   * Block until the transfer finishes and copy the received bytes into the
   * caller's string. The string is resized to the number of bytes received.
   *
   * @param out     destination buffer, resized to fit the received bytes
   *
   * @return        OK on success, otherwise the transfer error code
   */
  ErrorCode finish(std::string& out);

  /**
   * Block until the transfer finishes and copy the received bytes into a
   * caller-provided buffer.
   *
   * @param buf         destination buffer
   * @param capacity    capacity of buf in bytes
   * @param received    set to the number of bytes received
   *
   * @return            OK on success; ERROR if the received size exceeds
   *                    capacity; otherwise the transfer error code
   */
  ErrorCode finish(void* buf, size_t capacity, size_t& received);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
}  // namespace wdt
}  // namespace facebook
