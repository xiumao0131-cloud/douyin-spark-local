import pytest

from spark.engine import cookies


def test_parse_header_string():
    parsed = cookies.parse_cookie_input("sessionid=abc; sid_guard=xyz; foo=bar")
    names = {item["name"] for item in parsed}
    assert {"sessionid", "sid_guard", "foo"} <= names
    assert all(item["domain"].startswith(".") for item in parsed)


def test_parse_json_array():
    raw = '[{"name": "sessionid", "value": "abc", "domain": "douyin.com"}]'
    parsed = cookies.parse_cookie_input(raw)
    assert parsed[0]["name"] == "sessionid"
    assert parsed[0]["domain"] == ".douyin.com"


def test_parse_single_pair():
    parsed = cookies.parse_cookie_input("sessionid=abc")
    assert parsed[0]["name"] == "sessionid"


def test_parse_rejects_garbage():
    with pytest.raises(cookies.CookieParseError):
        cookies.parse_cookie_input("这不是cookie")


def test_require_auth_cookies_rejects_when_missing():
    parsed = cookies.parse_cookie_input("foo=1; bar=2")
    with pytest.raises(cookies.CookieParseError) as excinfo:
        cookies.require_auth_cookies(parsed)
    assert excinfo.value.category == "cookie_auth_missing"


def test_require_auth_cookies_accepts_known_set():
    parsed = cookies.parse_cookie_input("sessionid=a; uid_tt=b")
    names = cookies.require_auth_cookies(parsed)
    assert "sessionid" in names
