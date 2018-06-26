//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// network_io_wrappers.h
//
// Identification: src/include/network/network_io_wrappers.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <openssl/ssl.h>
#include <memory>
#include <utility>
#include "common/exception.h"
#include "common/utility.h"
<<<<<<< HEAD
#include "network/network_types.h"
=======
>>>>>>> a045cfc95bf349742a8101aee65e22efd9ec8096
#include "network/marshal.h"

namespace peloton {
namespace network {

/**
 * A network io wrapper provides an interface for interacting with a client
 * connection.
 *
 * Underneath the hood the wrapper buffers read and write, and can support posix
 * and ssl reads and writes to the socket, depending on the concrete type at
 * runtime.
 *
 * Because the buffers are large and expensive to allocate on fly, they are
 * reused. Consequently, initialization of this class is handled by a factory
 * class. @see NetworkIoWrapperFactory
 */
class NetworkIoWrapper {
  friend class NetworkIoWrapperFactory;
 public:
  virtual bool SslAble() const = 0;
  // TODO(Tianyu): Change and document after we refactor protocol handler
  virtual Transition FillReadBuffer() = 0;
  virtual Transition FlushWriteBuffer(WriteBuffer &wbuf) = 0;
  virtual Transition Close() = 0;

  inline int GetSocketFd() { return sock_fd_; }
  inline std::shared_ptr<ReadBuffer> GetReadBuffer() { return in_; }
  inline std::shared_ptr<WriteQueue> GetWriteQueue() { return out_; }
  Transition FlushAllWrites();
  inline bool ShouldFlush() { return out_->ShouldFlush(); }
  // TODO(Tianyu): Make these protected when protocol handler refactor is
  // complete
  NetworkIoWrapper(int sock_fd, std::shared_ptr<ReadBuffer> &in,
                   std::shared_ptr<WriteQueue> &out)
      : sock_fd_(sock_fd),
        in_(std::move(in)),
        out_(std::move(out)) {
    in_->Reset();
    out_->Reset();
  }

  DISALLOW_COPY(NetworkIoWrapper)

  NetworkIoWrapper(NetworkIoWrapper &&other) = default;

  int sock_fd_;
  std::shared_ptr<ReadBuffer> in_;
  std::shared_ptr<WriteQueue> out_;
};

/**
 * A Network IoWrapper specialized for dealing with posix sockets.
 */
class PosixSocketIoWrapper : public NetworkIoWrapper {
 public:
  PosixSocketIoWrapper(int sock_fd, std::shared_ptr<ReadBuffer> in,
                       std::shared_ptr<WriteQueue> out);


  inline bool SslAble() const override { return false; }
  Transition FillReadBuffer() override;
  Transition FlushWriteBuffer(WriteBuffer &wbuf) override;
  inline Transition Close() override {
    peloton_close(sock_fd_);
    return Transition::PROCEED;
  }
};

/**
 * NetworkIoWrapper specialized for dealing with ssl sockets.
 */
class SslSocketIoWrapper : public NetworkIoWrapper {
 public:
  // Realistically, an SslSocketIoWrapper is always derived from a
  // PosixSocketIoWrapper, as the handshake process happens over posix sockets.
  SslSocketIoWrapper(NetworkIoWrapper &&other, SSL *ssl)
    : NetworkIoWrapper(std::move(other)), conn_ssl_context_(ssl) {}

  inline bool SslAble() const override { return true; }
  Transition FillReadBuffer() override;
  Transition FlushWriteBuffer(WriteBuffer &wbuf) override;
  Transition Close() override;

 private:
  friend class NetworkIoWrapperFactory;
  SSL *conn_ssl_context_;
};
}  // namespace network
}  // namespace peloton
