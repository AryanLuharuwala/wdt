/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE  // for memfd_create
#endif

#include <wdt/buffer/WdtBuffer.h>

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <wdt/Receiver.h>
#include <wdt/Sender.h>
#include <wdt/Wdt.h>

#include <cerrno>
#include <cstring>

namespace facebook {
namespace wdt {

namespace {

/// Relative name used for the single buffer "file" exchanged by this layer.
const char* const kBufferName = "wdt_buffer";

/// RAM-backed scratch root. tmpfs means received bytes never hit physical disk.
const char* const kScratchRoot = "/dev/shm";

/// Write the whole buffer into an in-memory (memfd) fd and return it. The
/// returned fd is positioned at offset 0 and owned by the caller (must close).
/// Returns -1 on failure.
int makeMemoryFd(const void* buf, size_t len) {
  int fd = ::memfd_create(kBufferName, MFD_CLOEXEC);
  if (fd < 0) {
    WPLOG(ERROR) << "memfd_create failed";
    return -1;
  }
  size_t written = 0;
  const char* data = static_cast<const char*>(buf);
  while (written < len) {
    ssize_t n = ::write(fd, data + written, len - written);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      WPLOG(ERROR) << "write to memfd failed";
      ::close(fd);
      return -1;
    }
    written += n;
  }
  if (::lseek(fd, 0, SEEK_SET) < 0) {
    WPLOG(ERROR) << "lseek on memfd failed";
    ::close(fd);
    return -1;
  }
  return fd;
}

}  // namespace

ErrorCode BufferSender::send(const WdtTransferRequest& connectionRequest,
                             const void* buf, size_t len) {
  int fd = makeMemoryFd(buf, len);
  if (fd < 0) {
    return MEMORY_ALLOCATION_ERROR;
  }
  // Transmit the in-memory (memfd) fd through the in-process sender. wdt reads
  // from the fd over its existing FileByteSource fd path, so nothing is read
  // from disk and no process is spawned. (InMemoryByteSource is NOT used here:
  // wiring it in would require a custom SourceQueue and is deferred -- @see
  // wdt/buffer/WdtBuffer.h for the full mechanism + portability notes.)
  WdtTransferRequest req = connectionRequest;
  req.fileInfo.clear();
  req.fileInfo.emplace_back(fd, static_cast<int64_t>(len), kBufferName);
  req.disableDirectoryTraversal = true;

  Sender sender(req);
  WdtTransferRequest processed = sender.init();
  if (processed.errorCode != OK) {
    ::close(fd);
    return processed.errorCode;
  }
  std::unique_ptr<TransferReport> report = sender.transfer();
  ::close(fd);
  return report->getSummary().getErrorCode();
}

ErrorCode BufferSender::send(const std::string& url, const void* buf,
                             size_t len) {
  WdtTransferRequest req(url);
  if (req.errorCode != OK) {
    WLOG(ERROR) << "Could not parse wdt url: " << errorCodeToStr(req.errorCode);
    return req.errorCode;
  }
  return send(req, buf, len);
}

/// Owns the underlying receiver and its RAM-backed scratch directory.
struct BufferReceiver::Impl {
  std::unique_ptr<Receiver> receiver;
  WdtTransferRequest connectionRequest;
  std::string scratchDir;
  bool started{false};

  ~Impl() {
    cleanupScratch();
  }

  void cleanupScratch() {
    if (scratchDir.empty()) {
      return;
    }
    std::string file = scratchDir + "/" + kBufferName;
    ::unlink(file.c_str());
    ::rmdir(scratchDir.c_str());
    scratchDir.clear();
  }
};

BufferReceiver::BufferReceiver() : impl_(new Impl()) {
}

BufferReceiver::~BufferReceiver() = default;

ErrorCode BufferReceiver::start(int numPorts) {
  // Unique RAM-backed scratch directory; received bytes live in tmpfs only.
  std::string dirTemplate = std::string(kScratchRoot) + "/wdt_buffer_XXXXXX";
  std::vector<char> tmpl(dirTemplate.begin(), dirTemplate.end());
  tmpl.push_back('\0');
  if (::mkdtemp(tmpl.data()) == nullptr) {
    WPLOG(ERROR) << "mkdtemp failed under " << kScratchRoot;
    return ERROR;
  }
  impl_->scratchDir.assign(tmpl.data());

  WdtTransferRequest req(/* start port */ 0, numPorts, impl_->scratchDir);
  impl_->receiver.reset(new Receiver(req));
  impl_->connectionRequest = impl_->receiver->init();
  if (impl_->connectionRequest.errorCode != OK) {
    return impl_->connectionRequest.errorCode;
  }
  ErrorCode code = impl_->receiver->transferAsync();
  impl_->started = (code == OK);
  return code;
}

std::string BufferReceiver::getConnectionUrl() const {
  if (!impl_->started) {
    return std::string();
  }
  return impl_->connectionRequest.genWdtUrlWithSecret();
}

const WdtTransferRequest& BufferReceiver::getConnectionRequest() const {
  return impl_->connectionRequest;
}

ErrorCode BufferReceiver::finish(std::string& out) {
  if (!impl_->started) {
    return ERROR;
  }
  std::unique_ptr<TransferReport> report = impl_->receiver->finish();
  ErrorCode code = report->getSummary().getErrorCode();
  if (code != OK) {
    impl_->cleanupScratch();
    return code;
  }
  // Read the staged bytes back out of tmpfs into the caller's buffer.
  std::string file = impl_->scratchDir + "/" + kBufferName;
  int fd = ::open(file.c_str(), O_RDONLY);
  if (fd < 0) {
    WPLOG(ERROR) << "Could not open received buffer " << file;
    impl_->cleanupScratch();
    return FILE_WRITE_ERROR;
  }
  struct stat st;
  if (::fstat(fd, &st) != 0) {
    WPLOG(ERROR) << "fstat failed on " << file;
    ::close(fd);
    impl_->cleanupScratch();
    return FILE_WRITE_ERROR;
  }
  out.resize(st.st_size);
  size_t readSoFar = 0;
  while (readSoFar < out.size()) {
    ssize_t n = ::read(fd, &out[readSoFar], out.size() - readSoFar);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      WPLOG(ERROR) << "read failed on " << file;
      ::close(fd);
      impl_->cleanupScratch();
      return FILE_WRITE_ERROR;
    }
    if (n == 0) {
      break;
    }
    readSoFar += n;
  }
  out.resize(readSoFar);
  ::close(fd);
  impl_->cleanupScratch();
  return OK;
}

ErrorCode BufferReceiver::finish(void* buf, size_t capacity, size_t& received) {
  received = 0;
  std::string out;
  ErrorCode code = finish(out);
  if (code != OK) {
    return code;
  }
  if (out.size() > capacity) {
    WLOG(ERROR) << "Received " << out.size() << " bytes but caller buffer only "
                << "holds " << capacity;
    return ERROR;
  }
  ::memcpy(buf, out.data(), out.size());
  received = out.size();
  return OK;
}
}  // namespace wdt
}  // namespace facebook
