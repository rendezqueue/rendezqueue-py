import json

import pytest

from rendezqueue.plyswap import Message
from rendezqueue.plyswap_wire import (
    decode_bytes,
    decode_message,
    decode_request,
    encode_bytes,
    encode_message,
    encode_request,
    query_from_target,
)


@pytest.mark.parametrize("flags", range(8))
def test_every_bitmap_preserves_all_bytes_and_presence(flags):
    message = Message(
        bytes(range(256)),
        peer_xid=b"\x00",
        next_xid=b"\x00\xff",
        values=(b"", bytes(range(255, -1, -1))),
    )
    room = bytes(range(256))
    body = encode_request(message, room, flags)
    envelope = decode_request(body)
    assert envelope.room == room
    assert envelope.message == message
    assert envelope.b64 == flags
    assert decode_message(encode_message(message, flags)) == message
    obj = json.loads(body)
    assert list(obj) == (["b64"] if flags else []) + [
        "room",
        "self",
        "peer",
        "next",
        "values",
    ]
    assert obj.get("b64", 0) == flags
    assert obj["self"] == (
        encode_bytes(message.self_xid)
        if flags & 2
        else message.self_xid.decode("latin1")
    )
    assert obj["peer"] == (
        encode_bytes(message.peer_xid)
        if flags & 2
        else message.peer_xid.decode("latin1")
    )
    assert message.next_xid
    assert obj["next"] == (
        encode_bytes(message.next_xid)
        if flags & 2
        else message.next_xid.decode("latin1")
    )
    assert obj["values"][1] == (
        encode_bytes(message.values[1])
        if flags & 1
        else message.values[1].decode("latin1")
    )
    assert obj["room"] == (encode_bytes(room) if flags & 4 else room.decode("latin1"))


def test_emitted_field_order():
    message = Message(b"a", peer_xid=b"b", next_xid=b"a1", values=(b"hi",))
    assert (
        encode_request(message, b"room")
        == b'{"room":"room","self":"a","peer":"b","next":"a1","values":["hi"]}'
    )
    assert (
        encode_request(message, b"room", 7)
        == b'{"b64":7,"room":"cm9vbQ","self":"YQ","peer":"Yg","next":"YTE","values":["aGk"]}'
    )
    assert (
        encode_message(Message(b"a", next_xid=b"b1", values=(b"hi",)))
        == b'{"self":"a","next":"b1","values":["hi"]}'
    )
    assert (
        encode_message(Message(b"a", next_xid=b"b1", values=(b"hi",)), 3)
        == b'{"b64":3,"self":"YQ","next":"YjE","values":["aGk"]}'
    )


@pytest.mark.parametrize("value", [b"", b"\x00\xff\x80", b"hello", bytes(range(256))])
def test_codec_roundtrip(value):
    assert decode_bytes(encode_bytes(value)) == value
    original = Message(b"a", peer_xid=b"", next_xid=b"", values=(b"", value))
    assert decode_message(encode_message(original)) == original


def test_omitted_empty_values_and_serialization():
    assert decode_message(b'{"self":"a"}') == decode_message(
        b'{"self":"a","values":[]}'
    )
    assert decode_message(b'{"self":"a","values":[""]}').values == (b"",)
    assert encode_message(Message(b"a", peer_xid=b"", next_xid=b"")) == b'{"self":"a"}'


@pytest.mark.parametrize(
    "body",
    [
        b"null",
        b"[]",
        b'""',
        b"{}",
        b'{"self":""}',
        b'{"self":null}',
        b'{"self":true}',
        b'{"self":0}',
        b'{"self":[]}',
        b'{"self":"YQ","self":"Yg"}',
        b'{"self":"YQ","peer":"Yg","peer":"Yg"}',
        b'{"self":"YQ","values":[],"values":[]}',
        b'{"self":"YQ","next":"Yg","next":"Yg"}',
        b'{"self":"YQ","values":null}',
        b'{"self":"YQ","values":""}',
        b'{"self":"YQ","values":[0]}',
        b'{"self":"YQ","peer":null}',
        b'{"self":"YQ","next":false}',
        b'{"self":"YQ","ttl":20}',
        b'{"self":"YQ","room":""}',
        b'{"self":"YQ","offset":0}',
        b'{"self":"\xff"}',
        b'{"self":"YQ","values":[NaN]}',
        b'{"self":"YQ"} trailing',
        '{"self":"YQ"}'.encode("utf-16"),
        b'\xef\xbb\xbf{"self":"YQ"}',
    ],
)
def test_invalid_json_and_message_fields(body):
    with pytest.raises(ValueError):
        decode_message(body)


def test_plain_strings_are_not_guessed_to_be_base64():
    assert decode_message(b'{"self":"YQ","values":["aGk"]}') == Message(
        b"YQ", values=(b"aGk",)
    )
    assert decode_message(b'{"self":"YQ","values":["aGk"],"b64":3}') == Message(
        b"a", values=(b"hi",)
    )


@pytest.mark.parametrize("value", ["/w==", "/w", "_w==", "_w"])
def test_valid_base64_variants(value):
    assert decode_bytes(value) == b"\xff"
    assert (
        decode_message(json.dumps({"self": value, "b64": 2}).encode()).self_xid
        == b"\xff"
    )
    assert (
        decode_message(json.dumps({"self": value}).encode()).self_xid == value.encode()
    )


@pytest.mark.parametrize(
    "value", ["YR", "_x", "_x==", "A", "YQ=", "YQ===", "YQ\n", "Y=Q=", "@w"]
)
def test_invalid_base64_is_rejected(value):
    with pytest.raises(ValueError):
        decode_bytes(value)
    # The message boundary must actually honor the self encoding bit too.
    with pytest.raises(ValueError):
        decode_message(json.dumps({"b64": 2, "self": value}).encode())


@pytest.mark.parametrize("flags", [True, False, -1, 8, 1.0, None, "3"])
def test_invalid_bitmap(flags):
    with pytest.raises(ValueError):
        decode_message(json.dumps({"self": "a", "b64": flags}).encode())
    with pytest.raises(ValueError):
        encode_request(Message(b"a"), b"room", flags)


@pytest.mark.parametrize(
    "body",
    [b'{"self":"a","b64":0,"b64":0}', b'{"self":"\\u0100"}', b'{"self":"\\ud800"}'],
)
def test_duplicate_bitmap_and_non_byte_strings(body):
    with pytest.raises(ValueError):
        decode_message(body)


def test_room_sources_and_encodings_are_equivalent():
    raw = decode_request(b'{"room":"\\u00ff","self":"a"}')
    encoded = decode_request(b'{"room":"/w==","self":"YQ","b64":6}')
    query = decode_request(b'{"self":"a"}', query_from_target("/?room=%C3%BF"))
    assert raw.room == encoded.room == query.room == b"\xff"
    assert raw.message == encoded.message == query.message == Message(b"a")
    assert (
        decode_request(
            b'{"self":"a","b64":4}', query_from_target("/?room=%2Fw%3D%3D")
        ).room
        == b"\xff"
    )
    assert query_from_target("/?room=a+b%2Bc%2520") == "a b+c%20"


@pytest.mark.parametrize(
    "body,query",
    [
        (b'{"self":"a"}', None),
        (b'{"room":"","self":"a"}', None),
        (b'{"room":null,"self":"a"}', None),
        (b'{"room":"room","room":"room","self":"a"}', None),
        (b'{"room":"room","self":"a"}', "room"),
        (b'{"self":"a"}', ""),
        (b'{"room":"room","self":"a","extra":1}', None),
    ],
)
def test_invalid_room_envelopes(body, query):
    with pytest.raises(ValueError):
        decode_request(body, query)


@pytest.mark.parametrize(
    "target",
    [
        "/?",
        "/?room",
        "/?key=room",
        "/?room=x&key=x",
        "/?room=x&room=x",
        "/?room=x&extra=y",
        "/?extra=x",
        "/?room=%",
        "/?room=%0G",
        "/?room=%FF",
        "/?room=ÿ",
    ],
)
def test_strict_query_admission(target):
    with pytest.raises(ValueError):
        query_from_target(target)
