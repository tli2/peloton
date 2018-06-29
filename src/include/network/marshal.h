//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// marshal.h
//
// Identification: src/include/network/marshal.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/value_factory.h"
#include "network/network_io_utils.h"


#define BUFFER_INIT_SIZE 100
namespace peloton {
namespace network {
class InputPacket {
 public:
  NetworkMessageType msg_type;         // header
  size_t len;                          // size of packet without header
  size_t ptr;                          // ByteBuf cursor
  ByteBuf::const_iterator begin, end;  // start and end iterators of the buffer
  bool header_parsed;                  // has the header been parsed
  bool is_initialized;                 // has the packet been initialized
  bool is_extended;  // check if we need to use the extended buffer

 private:
  ByteBuf extended_buffer_;  // used to store packets that don't fit in rbuf

 public:
  // reserve buf's size as maximum packet size
  inline InputPacket() { Reset(); }

  // Create a packet for prepared statement parameter data before parsing it
  inline InputPacket(int len, std::string &val) {
    Reset();
    // Copy the data from string to packet buf
    this->len = len;
    extended_buffer_.resize(len);
    PELOTON_MEMCPY(extended_buffer_.data(), val.data(), len);
    InitializePacket();
  }

  inline void Reset() {
    is_initialized = header_parsed = is_extended = false;
    len = ptr = 0;
    msg_type = NetworkMessageType::NULL_COMMAND;
    extended_buffer_.clear();
  }

  inline void ReserveExtendedBuffer() {
    // grow the buffer's capacity to len
    extended_buffer_.reserve(len);
  }

  /* checks how many more bytes the extended packet requires */
  inline size_t ExtendedBytesRequired() {
    return len - extended_buffer_.size();
  }

  inline void AppendToExtendedBuffer(ByteBuf::const_iterator start,
                                     ByteBuf::const_iterator end) {
    extended_buffer_.insert(std::end(extended_buffer_), start, end);
  }

  inline void InitializePacket(size_t &pkt_start_index,
                               ByteBuf::const_iterator rbuf_begin) {
    this->begin = rbuf_begin + pkt_start_index;
    this->end = this->begin + len;
    is_initialized = true;
  }

  inline void InitializePacket() {
    this->begin = extended_buffer_.begin();
    this->end = extended_buffer_.end();
    PELOTON_ASSERT(extended_buffer_.size() == len);
    is_initialized = true;
  }

  ByteBuf::const_iterator Begin() { return begin; }

  ByteBuf::const_iterator End() { return end; }
};

struct OutputPacket {
  ByteBuf buf;                  // stores packet contents
  size_t len;                   // size of packet
  size_t ptr;                   // ByteBuf cursor, which is used for get and put
  NetworkMessageType msg_type;  // header

  bool single_type_pkt;  // there would be only a pkt type being written to the
  // buffer when this flag is true
  bool skip_header_write;  // whether we should write header to soc ket wbuf
  size_t write_ptr;        // cursor used to write packet content to socket wbuf

  // TODO could packet be reused?
  inline void Reset() {
    buf.resize(BUFFER_INIT_SIZE);
    buf.shrink_to_fit();
    buf.clear();
    single_type_pkt = false;
    len = ptr = write_ptr = 0;
    msg_type = NetworkMessageType::NULL_COMMAND;
    skip_header_write = true;
  }
};

/*
 * Marshallers
 */

/* packet_put_byte - used to write a single byte into a packet */
extern void PacketPutByte(OutputPacket *pkt, const uchar c);

/* packet_put_string - used to write a string into a packet */
extern void PacketPutStringWithTerminator(OutputPacket *pkt,
                                          const std::string &str);

/* packet_put_int - used to write a single int into a packet */
extern void PacketPutInt(OutputPacket *pkt, int n, int base);

/* packet_put_cbytes - used to write a uchar* into a packet */
extern void PacketPutCbytes(OutputPacket *pkt, const uchar *b, int len);

/* packet_put_bytes - used to write a uchar vector into a packet */
extern void PacketPutString(OutputPacket *pkt, const std::string &data);

/*
 * Unmarshallers
 */

/* Copy len bytes from the position indicated by begin to an array */
extern uchar *PacketCopyBytes(ByteBuf::const_iterator begin, int len);
/*
 * packet_get_int -  Parse an int out of the head of the
 * 	packet. "base" bytes determine the number of bytes of integer
 * 	we are parsing out.
 */
extern int PacketGetInt(InputPacket *pkt, uchar base);

/*
 * packet_get_string - parse out a string of size len.
 * 		if len=0? parse till the end of the string
 */
extern void PacketGetString(InputPacket *pkt, size_t len, std::string &result);

/* packet_get_bytes - Parse out "len" bytes of pkt as raw bytes */
extern void PacketGetBytes(InputPacket *pkt, size_t len, ByteBuf &result);

/* packet_get_byte - Parse out a single bytes from pkt */
extern void PacketGetByte(InputPacket *rpkt, uchar &result);

/*
 * get_string_token - used to extract a string token
 * 		from an unsigned char vector
 */
extern void GetStringToken(InputPacket *pkt, std::string &result);

// TODO(Tianyu): These dumb things are here because copy_executor somehow calls
// our network layer. This should NOT be the case. Will remove.
size_t OldReadParamType(
    InputPacket *pkt, int num_params, std::vector<int32_t> &param_types) {
  auto begin = pkt->ptr;
  // get the type of each parameter
  for (int i = 0; i < num_params; i++) {
    int param_type = PacketGetInt(pkt, 4);
    param_types[i] = param_type;
  }
  auto end = pkt->ptr;
  return end - begin;
}

size_t OldReadParamFormat(InputPacket *pkt,
                          int num_params_format,
                          std::vector<int16_t> &formats) {
  auto begin = pkt->ptr;
  // get the format of each parameter
  for (int i = 0; i < num_params_format; i++) {
    formats[i] = PacketGetInt(pkt, 2);
  }
  auto end = pkt->ptr;
  return end - begin;
}

// For consistency, this function assumes the input vectors has the correct size
size_t OldReadParamValue(
    InputPacket *pkt, int num_params, std::vector<int32_t> &param_types,
    std::vector<std::pair<type::TypeId, std::string>> &bind_parameters,
    std::vector<type::Value> &param_values, std::vector<int16_t> &formats) {
  auto begin = pkt->ptr;
  ByteBuf param;
  for (int param_idx = 0; param_idx < num_params; param_idx++) {
    int param_len = PacketGetInt(pkt, 4);
    // BIND packet NULL parameter case
    if (param_len == -1) {
      // NULL mode
      auto peloton_type = PostgresValueTypeToPelotonValueType(
          static_cast<PostgresValueType>(param_types[param_idx]));
      bind_parameters[param_idx] =
          std::make_pair(peloton_type, std::string(""));
      param_values[param_idx] =
          type::ValueFactory::GetNullValueByType(peloton_type);
    } else {
      PacketGetBytes(pkt, param_len, param);

      if (formats[param_idx] == 0) {
        // TEXT mode
        std::string param_str = std::string(std::begin(param), std::end(param));
        bind_parameters[param_idx] =
            std::make_pair(type::TypeId::VARCHAR, param_str);
        if ((unsigned int)param_idx >= param_types.size() ||
            PostgresValueTypeToPelotonValueType(
                (PostgresValueType)param_types[param_idx]) ==
                type::TypeId::VARCHAR) {
          param_values[param_idx] =
              type::ValueFactory::GetVarcharValue(param_str);
        } else {
          param_values[param_idx] =
              (type::ValueFactory::GetVarcharValue(param_str))
                  .CastAs(PostgresValueTypeToPelotonValueType(
                      (PostgresValueType)param_types[param_idx]));
        }
        PELOTON_ASSERT(param_values[param_idx].GetTypeId() !=
            type::TypeId::INVALID);
      } else {
        // BINARY mode
        PostgresValueType pg_value_type =
            static_cast<PostgresValueType>(param_types[param_idx]);
        LOG_TRACE("Postgres Protocol Conversion [param_idx=%d]", param_idx);
        switch (pg_value_type) {
          case PostgresValueType::TINYINT: {
            int8_t int_val = 0;
            for (size_t i = 0; i < sizeof(int8_t); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters[param_idx] =
                std::make_pair(type::TypeId::TINYINT, std::to_string(int_val));
            param_values[param_idx] =
                type::ValueFactory::GetTinyIntValue(int_val).Copy();
            break;
          }
          case PostgresValueType::SMALLINT: {
            int16_t int_val = 0;
            for (size_t i = 0; i < sizeof(int16_t); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters[param_idx] =
                std::make_pair(type::TypeId::SMALLINT, std::to_string(int_val));
            param_values[param_idx] =
                type::ValueFactory::GetSmallIntValue(int_val).Copy();
            break;
          }
          case PostgresValueType::INTEGER: {
            int32_t int_val = 0;
            for (size_t i = 0; i < sizeof(int32_t); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters[param_idx] =
                std::make_pair(type::TypeId::INTEGER, std::to_string(int_val));
            param_values[param_idx] =
                type::ValueFactory::GetIntegerValue(int_val).Copy();
            break;
          }
          case PostgresValueType::BIGINT: {
            int64_t int_val = 0;
            for (size_t i = 0; i < sizeof(int64_t); ++i) {
              int_val = (int_val << 8) | param[i];
            }
            bind_parameters[param_idx] =
                std::make_pair(type::TypeId::BIGINT, std::to_string(int_val));
            param_values[param_idx] =
                type::ValueFactory::GetBigIntValue(int_val).Copy();
            break;
          }
          case PostgresValueType::DOUBLE: {
            double float_val = 0;
            unsigned long buf = 0;
            for (size_t i = 0; i < sizeof(double); ++i) {
              buf = (buf << 8) | param[i];
            }
            PELOTON_MEMCPY(&float_val, &buf, sizeof(double));
            bind_parameters[param_idx] = std::make_pair(
                type::TypeId::DECIMAL, std::to_string(float_val));
            param_values[param_idx] =
                type::ValueFactory::GetDecimalValue(float_val).Copy();
            break;
          }
          case PostgresValueType::VARBINARY: {
            bind_parameters[param_idx] = std::make_pair(
                type::TypeId::VARBINARY,
                std::string(reinterpret_cast<char *>(&param[0]), param_len));
            param_values[param_idx] = type::ValueFactory::GetVarbinaryValue(
                &param[0], param_len, true);
            break;
          }
          default: {
            LOG_ERROR(
                "Binary Postgres protocol does not support data type '%s' [%d]",
                PostgresValueTypeToString(pg_value_type).c_str(),
                param_types[param_idx]);
            break;
          }
        }
        PELOTON_ASSERT(param_values[param_idx].GetTypeId() !=
            type::TypeId::INVALID);
      }
    }
  }
  auto end = pkt->ptr;
  return end - begin;
}

}  // namespace network
}  // namespace peloton
