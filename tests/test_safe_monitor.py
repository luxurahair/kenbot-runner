"""Offline regression checks for safe runner; no network, secrets or production writes."""
import io
import json
import unittest
import urllib.error
from unittest.mock import patch
from monitoring.safe_monitor import classify_http, probe, check_services, main

class FakeResponse:
    def __init__(self, code): self.status = code
    def __enter__(self): return self
    def __exit__(self, *args): return False

class SafeMonitorTests(unittest.TestCase):
    def test_http_200(self): self.assertEqual(classify_http(200), "HEALTHY")
    def test_http_401(self): self.assertEqual(classify_http(401), "AUTH_CONFIG")
    def test_http_403(self): self.assertEqual(classify_http(403), "AUTH_CONFIG")
    def test_http_404(self): self.assertEqual(classify_http(404), "ENDPOINT_CONFIG")
    def test_http_429(self): self.assertEqual(classify_http(429), "RATE_LIMIT")
    def test_http_500(self): self.assertEqual(classify_http(500), "SERVICE_ERROR")
    def test_http_503(self): self.assertEqual(classify_http(503), "SERVICE_ERROR")
    def test_http_301(self): self.assertEqual(classify_http(301), "HTTP_ERROR")
    def test_rejects_http(self): self.assertEqual(probe("http://example.com")["status"], "CONFIG_ERROR")
    def test_rejects_empty_url(self): self.assertEqual(probe("")["status"], "CONFIG_ERROR")
    def test_rejects_nonstring(self): self.assertEqual(probe(None)["status"], "CONFIG_ERROR")
    def test_success_one_attempt(self): self.assertEqual(probe("https://example.com", opener=lambda *a,**k:FakeResponse(200))["attempts"],1)
    def test_unauthorized_no_retry(self): self.assertEqual(probe("https://example.com",opener=lambda *a,**k:FakeResponse(401))["attempts"],1)
    def test_notfound_no_retry(self): self.assertEqual(probe("https://example.com",opener=lambda *a,**k:FakeResponse(404))["attempts"],1)
    def test_server_error_three_attempts(self): self.assertEqual(probe("https://example.com",opener=lambda *a,**k:FakeResponse(500),sleeper=lambda *_:None)["attempts"],3)
    def test_attempts_capped(self): self.assertEqual(probe("https://example.com",opener=lambda *a,**k:FakeResponse(500), attempts=50,sleeper=lambda *_:None)["attempts"],3)
    def test_attempts_minimum(self): self.assertEqual(probe("https://example.com",opener=lambda *a,**k:FakeResponse(500), attempts=0,sleeper=lambda *_:None)["attempts"],1)
    def test_network_failure(self):
        def fail(*a,**k): raise urllib.error.URLError("offline")
        self.assertEqual(probe("https://example.com",opener=fail,sleeper=lambda *_:None)["status"],"NETWORK_ERROR")
    def test_timeout_failure(self):
        def fail(*a,**k): raise TimeoutError()
        self.assertEqual(probe("https://example.com",opener=fail,sleeper=lambda *_:None)["status"],"NETWORK_ERROR")
    def test_os_error_failure(self):
        def fail(*a,**k): raise OSError("offline")
        self.assertEqual(probe("https://example.com",opener=fail,sleeper=lambda *_:None)["status"],"NETWORK_ERROR")
    def test_recovery_after_failure(self):
        calls = iter([FakeResponse(503),FakeResponse(200)])
        self.assertEqual(probe("https://example.com",opener=lambda *a,**k:next(calls),sleeper=lambda *_:None),{"status":"HEALTHY","attempts":2})
    def test_http_error_response(self):
        def fail(*a,**k): raise urllib.error.HTTPError("https://example.com", 429, "rate", {},None)
        self.assertEqual(probe("https://example.com",opener=fail,sleeper=lambda *_:None)["status"],"RATE_LIMIT")
    def test_services_multiple(self):
        r=check_services({"a":"https://example.com","b":"https://example.org"},opener=lambda *a,**k:FakeResponse(200))
        self.assertEqual(set(r),{"a","b"})
    def test_services_empty(self): self.assertEqual(check_services({}),{})
    def test_json_extra_rejected(self):
        with patch.dict("os.environ",{"KENBOT_EXTRA_HEALTH_URLS_JSON":"[1,2]"}):
            with patch("sys.stdout",new_callable=io.StringIO):
                self.assertEqual(main(),2)
    def test_invalid_extra_rejected(self):
        with patch.dict("os.environ",{"KENBOT_EXTRA_HEALTH_URLS_JSON":"{bad"}):
            with patch("sys.stdout",new_callable=io.StringIO):
                self.assertEqual(main(),2)
    def test_main_good(self):
        with patch("monitoring.safe_monitor.check_services",return_value={"x":{"status":"HEALTHY"}}),patch.dict("os.environ",{"KENBOT_EXTRA_HEALTH_URLS_JSON":""}),patch("sys.stdout",new_callable=io.StringIO):
            self.assertEqual(main(),0)
    def test_main_bad(self):
        with patch("monitoring.safe_monitor.check_services",return_value={"x":{"status":"SERVICE_ERROR"}}),patch.dict("os.environ",{"KENBOT_EXTRA_HEALTH_URLS_JSON":""}),patch("sys.stdout",new_callable=io.StringIO):
            self.assertEqual(main(),1)
    def test_main_empty_extra(self):
        with patch("monitoring.safe_monitor.check_services",return_value={"x":{"status":"HEALTHY"}}),patch.dict("os.environ",{"KENBOT_EXTRA_HEALTH_URLS_JSON":"   "}),patch("sys.stdout",new_callable=io.StringIO):
            self.assertEqual(main(),0)
    def test_main_adds_extra(self):
        with patch("monitoring.safe_monitor.check_services",return_value={"x":{"status":"HEALTHY"}}) as fn,patch.dict("os.environ",{"KENBOT_EXTRA_HEALTH_URLS_JSON":json.dumps({"second":"https://example.com/health"})}),patch("sys.stdout",new_callable=io.StringIO):
            self.assertEqual(main(),0)
            self.assertIn("second",fn.call_args.args[0])
if __name__=="__main__": unittest.main()
