#!/usr/bin/env ruby

$:.unshift('../lib')
require 'gearman'
require 'gearman/testlib'
require 'test/unit'

class ProtocolTest < Test::Unit::TestCase

  def test_encode_request
    payload = ["foo", "123", "bar"].join("\0")
    expected = "\0REQ" + [Gearman::Protocol::NUMS[:submit_job], payload.size].pack("NN") + payload
    assert_equal expected, Gearman::Protocol.encode_request(:submit_job, payload)
  end

  def test_decode_response
    response = "\0RES" + [Gearman::Protocol::NUMS[:work_data], 3].pack("NN") + "H:wrk.acme:1\0foo"
    packets = Gearman::Protocol.decode_response(response)
    assert_equal 1, packets.size
    assert_equal [:work_data, "H:wrk.acme:1", "foo"], packets[0]
  end

  def test_response_packet
    packet = [:work_data, "H:wrk.acme:1", "foo"]
    assert_equal packet, Gearman::Protocol.response_packet(*packet)
  end
end
