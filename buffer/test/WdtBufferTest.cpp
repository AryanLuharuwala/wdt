/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <wdt/Wdt.h>
#include <wdt/buffer/InMemoryByteSource.h>
#include <wdt/buffer/WdtBuffer.h>

#include <string>

namespace facebook {
namespace wdt {

/// How the sender is told where to connect: either via the connection url
/// (genWdtUrlWithSecret, which omits the source dir) or via the in-memory
/// connection request object (which carries every field wdt needs).
enum class Handoff { Url, Request };

/// Loopback: send a buffer to a localhost receiver and assert bytes match.
void loopback(const std::string& contents, Handoff handoff = Handoff::Url) {
  BufferReceiver receiver;
  ASSERT_EQ(OK, receiver.start(/* numPorts */ 3));

  if (handoff == Handoff::Url) {
    const std::string url = receiver.getConnectionUrl();
    ASSERT_FALSE(url.empty());
    WLOG(INFO) << "buffer receiver listening at " << url;
    ASSERT_EQ(OK, BufferSender::send(url, contents.data(), contents.size()));
  } else {
    const WdtTransferRequest& conn = receiver.getConnectionRequest();
    ASSERT_EQ(OK, conn.errorCode);
    ASSERT_EQ(OK, BufferSender::send(conn, contents.data(), contents.size()));
  }

  std::string received;
  ASSERT_EQ(OK, receiver.finish(received));
  EXPECT_EQ(contents, received);
}

TEST(WdtBuffer, SmallBuffer) {
  loopback("hello wdt buffer api");
}

TEST(WdtBuffer, EmptyBuffer) {
  loopback("");
}

TEST(WdtBuffer, LargeBuffer) {
  // A few MB to exercise multi-block streaming from memory.
  std::string contents;
  contents.reserve(4 * 1024 * 1024);
  for (int i = 0; i < 4 * 1024 * 1024; ++i) {
    contents.push_back(static_cast<char>('a' + (i % 26)));
  }
  loopback(contents);
}

/// Same round-trip but handing the sender the in-memory connection request
/// object instead of the url string.
TEST(WdtBuffer, RequestObjectHandoff) {
  loopback("sent via connection request object", Handoff::Request);
}

/// Directly exercise the in-memory ByteSource: it should hand out exactly the
/// caller's bytes, in network-buffer sized slices, then report finished().
TEST(WdtBuffer, InMemoryByteSourceStreaming) {
  const std::string contents = "abcdefghijklmnopqrstuvwxyz0123456789";
  SourceMetaData metadata;
  metadata.relPath = "mem";
  metadata.size = contents.size();

  InMemoryByteSource source(&metadata, contents.data(), contents.size(),
                            /* offset */ 0);
  ThreadCtx threadCtx(WdtOptions::get(), /* allocateBuffer */ true);
  ASSERT_EQ(OK, source.open(&threadCtx));

  std::string collected;
  while (!source.finished()) {
    int64_t size = 0;
    char* data = source.read(size);
    ASSERT_NE(nullptr, data);
    ASSERT_GT(size, 0);
    collected.append(data, size);
  }
  EXPECT_FALSE(source.hasError());
  EXPECT_EQ(contents, collected);
  source.close();
}

TEST(WdtBuffer, CallerProvidedBuffer) {
  BufferReceiver receiver;
  ASSERT_EQ(OK, receiver.start());

  const std::string contents = "into a fixed buffer";
  ASSERT_EQ(OK, BufferSender::send(receiver.getConnectionUrl(), contents.data(),
                                   contents.size()));

  char out[64];
  size_t received = 0;
  ASSERT_EQ(OK, receiver.finish(out, sizeof(out), received));
  EXPECT_EQ(contents.size(), received);
  EXPECT_EQ(contents, std::string(out, received));
}
}  // namespace wdt
}  // namespace facebook

int main(int argc, char* argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::wdt::Wdt::initializeWdt("wdt_buffer");
  return RUN_ALL_TESTS();
}
