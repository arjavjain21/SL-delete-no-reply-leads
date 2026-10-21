import base64
import contextlib
import importlib.util
import io
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import smartlead_consolidated_git as app


def response(status=200, body=None, headers=None):
    result = requests.Response()
    result.status_code = status
    result.headers.update(headers or {})
    result._content = json.dumps(body).encode() if body is not None else b''
    return result


class ConfigurationTests(unittest.TestCase):
    def test_missing_resend_key_has_actionable_github_annotation(self):
        settings = dict(os.environ, RESEND_API_KEY='', GITHUB_ACTIONS='true',
                        SENDER_EMAIL='sender@example.com', RECIPIENT_EMAILS='recipient@example.com')
        result = subprocess.run([sys.executable, str(ROOT / 'smartlead_consolidated_git.py'),
                                 '--mode', 'email-test'], env=settings, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn('::error::Missing required environment variables: RESEND_API_KEY', result.stdout)

    def test_empty_github_variables_use_defaults(self):
        with patch.dict(os.environ, {key: '' for key in [
            'TARGET_LEADS', 'DAYS_WITHOUT_ACTIVITY', 'EXCLUDE_CLIENT_IDS',
            'SMARTLEAD_REQUESTS_PER_MINUTE', 'DELETE_WORKERS',
        ]}):
            spec = importlib.util.spec_from_file_location('empty_settings', ROOT / 'smartlead_consolidated_git.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        self.assertEqual(module.TARGET_LEADS, 20000)
        self.assertEqual(module.DAYS_WITHOUT_ACTIVITY, 30)
        self.assertEqual(module.EXCLUDE_CLIENT_IDS, [1598])
        self.assertEqual(module.SMARTLEAD_REQUESTS_PER_MINUTE, 5400)

    def test_email_test_does_not_require_smartlead(self):
        with patch.multiple(app, API_KEY=None, SENDER_EMAIL='sender@example.com',
                            RESEND_API_KEY='test-only', RECIPIENT_EMAILS=['recipient@example.com']):
            with contextlib.redirect_stdout(io.StringIO()):
                app.validate_environment(require_smartlead=False)
            with self.assertRaisesRegex(ValueError, 'SMARTLEAD_API_KEY'):
                app.validate_environment()

    def test_rate_limit_above_account_limit_rejected(self):
        with patch.multiple(app, API_KEY='test-only', SENDER_EMAIL='sender@example.com',
                            RESEND_API_KEY='test-only', RECIPIENT_EMAILS=['recipient@example.com'],
                            SMARTLEAD_REQUESTS_PER_MINUTE=6001):
            with self.assertRaisesRegex(ValueError, 'between 1 and 6000'):
                app.validate_environment()


class RateLimitTests(unittest.TestCase):
    def test_requests_are_spaced_and_cooldown_is_shared(self):
        clock = [100.0]
        with patch.object(app.time, 'monotonic', side_effect=lambda: clock[0]), \
             patch.object(app.time, 'sleep', side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds)):
            limiter = app.RateLimiter(5400)
            limiter.wait()
            limiter.wait()
            self.assertAlmostEqual(clock[0], 100 + 60 / 5400)
            limiter.defer(10)
            limiter.wait()
            self.assertAlmostEqual(clock[0], 110 + 60 / 5400)

    def test_retry_after_seconds_and_http_date(self):
        self.assertEqual(app.retry_delay(response(429, headers={'Retry-After': '90'}), 1), 90)
        self.assertEqual(app.retry_delay(response(429, headers={'Retry-After': 'invalid'}), 2), 4)
        fixed = app.datetime(2026, 1, 1, tzinfo=app.timezone.utc)
        with patch.object(app, 'datetime') as date:
            date.now.return_value = fixed
            self.assertEqual(app.retry_delay(response(429, headers={'Retry-After': 'Thu, 01 Jan 2026 00:01:00 GMT'}), 1), 60)


class SmartLeadRequestTests(unittest.TestCase):
    def setUp(self):
        self.session = Mock()
        self.patches = [patch.object(app, 'get_http_session', return_value=self.session),
                        patch.object(app, 'smartlead_limiter')]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def test_throttle_retries_and_counts_each_attempt(self):
        self.session.request.side_effect = [response(429, headers={'Retry-After': '20'}), response(200)]
        result = app.send_request('GET', app.BASE_URL + '/campaigns')
        self.assertEqual(result.status_code, 200)
        self.assertEqual(app.smartlead_limiter.wait.call_count, 2)
        app.smartlead_limiter.defer.assert_called_once_with(20)

    def test_permanent_error_is_not_retried(self):
        self.session.request.return_value = response(400)
        self.assertEqual(app.send_request('GET', app.BASE_URL).status_code, 400)
        self.assertEqual(self.session.request.call_count, 1)

    def test_auth_error_aborts(self):
        self.session.request.return_value = response(401)
        with self.assertRaisesRegex(RuntimeError, 'authentication'):
            app.send_request('GET', app.BASE_URL)
        self.assertEqual(self.session.request.call_count, 1)

    def test_transport_exception_cannot_leak_key_in_logs(self):
        self.session.request.side_effect = requests.Timeout('URL?api_key=secret-test-value')
        logger = logging.getLogger('secret-check')
        with self.assertLogs(logger, level='WARNING') as captured:
            self.assertIsNone(app.send_request('DELETE', app.BASE_URL, logger=logger))
        self.assertNotIn('secret-test-value', '\n'.join(captured.output))
        self.assertEqual(app.smartlead_limiter.wait.call_count, app.MAX_RETRIES)

    def test_campaign_fetch_failure_is_not_empty_success(self):
        self.session.request.return_value = response(400)
        with self.assertRaisesRegex(RuntimeError, 'campaign fetch'):
            app.fetch_all_campaigns('test-only', logging.getLogger('test'))


class ResendTests(unittest.TestCase):
    def test_correct_payload_and_base64_attachment(self):
        with tempfile.TemporaryDirectory() as folder:
            attachment = Path(folder) / 'backup.csv'
            attachment.write_bytes(b'id,reply_count\n1,0\n')
            with patch.multiple(app, RESEND_API_KEY='unit-test-only', SENDER_EMAIL='sender@example.com',
                                RECIPIENT_EMAILS=['one@example.com', 'two@example.com']), \
                 patch.object(app.requests, 'post', return_value=response(200, {'id': 'mail-1'})) as post:
                self.assertTrue(app.send_email('Report', 'Completed', [str(attachment)]))
                args = post.call_args
                self.assertEqual(args.args[0], 'https://api.resend.com/emails')
                self.assertEqual(args.kwargs['headers']['Authorization'], 'Bearer unit-test-only')
                payload = args.kwargs['json']
                self.assertEqual(payload['from'], 'sender@example.com')
                self.assertEqual(payload['to'], ['one@example.com', 'two@example.com'])
                self.assertEqual(base64.b64decode(payload['attachments'][0]['content']), attachment.read_bytes())

    def test_retry_reuses_idempotency_key_and_payload(self):
        with patch.object(app.requests, 'post', side_effect=[requests.Timeout(), response(429, headers={'Retry-After': '10'}), response(200)]) as post, \
             patch.object(app.time, 'sleep') as sleep:
            self.assertTrue(app.send_email('Report', 'Body'))
            calls = post.call_args_list
            self.assertEqual(len({c.kwargs['headers']['Idempotency-Key'] for c in calls}), 1)
            self.assertEqual(calls[0].kwargs['json'], calls[-1].kwargs['json'])
            sleep.assert_any_call(10)

    def test_invalid_sender_fails_without_retry(self):
        with patch.object(app.requests, 'post', return_value=response(403)) as post:
            self.assertFalse(app.send_email('Report', 'Body'))
        self.assertEqual(post.call_count, 1)

    def test_attachment_limit_preserves_report_with_artifact_notice(self):
        with tempfile.TemporaryDirectory() as folder:
            attachment = Path(folder) / 'large.csv'
            attachment.write_text('too big')
            with patch.object(app, 'MAX_EMAIL_ATTACHMENT_BYTES', 1), \
                 patch.object(app.requests, 'post', return_value=response(200)) as post:
                self.assertTrue(app.send_email('Report', 'Body', [str(attachment)]))
            payload = post.call_args.kwargs['json']
            self.assertNotIn('attachments', payload)
            self.assertIn('artifacts', payload['text'])


class ProcessingTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        original = os.getcwd()
        os.chdir(folder.name)
        self.addCleanup(os.chdir, original)
        self.processor = app.SmartLeadProcessor('unit-test-only', logging.getLogger('processing'), 'run.log')
        self.campaign = {'id': 101, 'name': 'Paused campaign', 'status': 'PAUSED', 'client_id': 7,
                         'created_at': '2020-01-01T00:00:00.000+0000', 'updated_at': '2020-01-01T00:00:00.000+0000'}

    def prepare(self, campaigns=None):
        Path('leads.csv').write_text('id,reply_count\n123456789012345678,0\n222,1\n333,0\n')
        with patch.object(app, 'fetch_all_campaigns', return_value=campaigns if campaigns is not None else [self.campaign]), \
             patch.object(app, 'export_leads_csv', return_value='leads.csv'):
            self.processor.run_full_process(prepare_only=True)

    def test_prepare_never_deletes_or_emails_and_persists_backup(self):
        with patch.object(self.processor, 'delete_leads') as delete, patch.object(app, 'send_email') as email:
            self.prepare()
        delete.assert_not_called()
        email.assert_not_called()
        state = json.loads(Path('run-state.json').read_text())
        backup = pd.read_csv(state['backup_file'], dtype={'id': 'string'})
        self.assertEqual(backup['id'].tolist(), ['123456789012345678', '333'])
        self.assertEqual(state['execution_stats']['leads_backed_up'], 2)

    def test_execute_prepared_backup_and_send_report(self):
        self.prepare()
        with patch.object(self.processor, 'delete_single_lead', return_value=True) as delete, \
             patch.object(app, 'send_email', return_value=True) as email:
            self.processor.run_prepared_process()
        self.assertEqual(delete.call_count, 2)
        self.assertEqual(self.processor.execution_stats['leads_deleted_success'], 2)
        email.assert_called_once()
        self.assertIn('Deletion Complete', email.call_args.args[0])

    def test_dry_run_sends_report_without_deleting(self):
        self.prepare()
        with patch.object(self.processor, 'delete_leads') as delete, patch.object(app, 'send_email', return_value=True) as email:
            self.processor.run_prepared_process(dry_run=True)
        delete.assert_not_called()
        self.assertIn('Dry Run', email.call_args.args[0])
        self.assertIn('no leads were deleted', email.call_args.args[1])

    def test_no_eligible_campaigns_is_successful_no_op(self):
        self.prepare(campaigns=[])
        with patch.object(self.processor, 'delete_leads') as delete, patch.object(app, 'send_email', return_value=True):
            self.processor.run_prepared_process()
        delete.assert_not_called()
        self.assertEqual(self.processor.execution_stats['leads_backed_up'], 0)

    def test_excluded_client_and_active_campaign_not_exported(self):
        self.prepare(campaigns=[dict(self.campaign, client_id=1598), dict(self.campaign, status='ACTIVE')])
        self.assertEqual(self.processor.execution_stats['campaigns_filtered'], 0)

    def test_reply_lead_or_missing_id_refuses_entire_deletion(self):
        for lead_id, reply_count in [('123', 1), (None, 0)]:
            with self.subTest(lead_id=lead_id, replies=reply_count), \
                 patch.object(self.processor, 'delete_single_lead') as delete:
                with self.assertRaises(ValueError):
                    self.processor.delete_leads(pd.DataFrame([{'Campaign ID': '1', 'id': lead_id, 'reply_count': reply_count}]))
                delete.assert_not_called()

    def test_changed_backup_refuses_deletion(self):
        self.prepare()
        state = json.loads(Path('run-state.json').read_text())
        Path(state['backup_file']).write_text('id,reply_count,Campaign ID\n1,0,101\n')
        with patch.object(self.processor, 'delete_leads') as delete:
            with self.assertRaisesRegex(ValueError, 'checksum changed'):
                self.processor.run_prepared_process()
        delete.assert_not_called()

    def test_string_client_id_and_iso_timestamp_supported(self):
        self.prepare(campaigns=[dict(self.campaign, client_id='1598', created_at='2020-01-01T00:00:00Z')])
        self.assertEqual(self.processor.execution_stats['campaigns_filtered'], 0)

    def test_partial_delete_failure_is_not_reported_as_success(self):
        self.prepare()
        with patch.object(self.processor, 'delete_single_lead', side_effect=[True, False]), \
             patch.object(app, 'send_email') as email:
            with self.assertRaisesRegex(RuntimeError, 'deletions failed'):
                self.processor.run_prepared_process()
        email.assert_not_called()

    def test_report_delivery_failure_fails_run(self):
        self.prepare(campaigns=[])
        with patch.object(app, 'send_email', return_value=False):
            with self.assertRaisesRegex(RuntimeError, 'completion report'):
                self.processor.run_prepared_process()

    def test_delete_404_and_204_are_success_without_response_truthiness_bug(self):
        for status in [200, 204, 404]:
            with self.subTest(status=status), patch.object(app, 'send_request', return_value=response(status)):
                self.assertTrue(self.processor.delete_single_lead('1', '2'))

    def test_export_error_aborts_before_delete_and_reports_failure(self):
        with patch.object(app, 'fetch_all_campaigns', return_value=[self.campaign]), \
             patch.object(app, 'export_leads_csv', side_effect=RuntimeError('export failed')), \
             patch.object(self.processor, 'delete_leads') as delete, \
             patch.object(app, 'send_email', return_value=True) as email:
            with self.assertRaisesRegex(RuntimeError, 'export failed'):
                self.processor.run_full_process()
        delete.assert_not_called()
        self.assertIn('FAILED', email.call_args.args[0])


class HTTPIntegrationTests(unittest.TestCase):
    def test_full_cleanup_uses_real_http_and_preserves_backups(self):
        deleted = []
        messages = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                if '/leads-export' in self.path:
                    body = b'id,reply_count\n111,0\n222,1\n333,0\n'
                    content_type = 'text/csv'
                else:
                    body = json.dumps([{'id': 101, 'name': 'Test campaign', 'status': 'COMPLETED',
                                        'client_id': 7, 'created_at': '2020-01-01T00:00:00Z',
                                        'updated_at': '2020-01-01T00:00:00Z'}]).encode()
                    content_type = 'application/json'
                self.send_response(200)
                self.send_header('Content-Type', content_type)
                self.end_headers()
                self.wfile.write(body)

            def do_DELETE(self):
                deleted.append(self.path.split('?')[0])
                self.send_response(204)
                self.end_headers()

            def do_POST(self):
                messages.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"id":"local-test-message"}')

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(server.server_close)
        self.addCleanup(worker.join)
        self.addCleanup(server.shutdown)
        with tempfile.TemporaryDirectory() as folder:
            original = os.getcwd()
            try:
                os.chdir(folder)
                endpoint = f'http://127.0.0.1:{server.server_port}'
                with patch.multiple(app, BASE_URL=endpoint, RESEND_URL=endpoint + '/emails',
                                    RESEND_API_KEY='local-test-only', SENDER_EMAIL='sender@example.com',
                                    RECIPIENT_EMAILS=['recipient@example.com'],
                                    http_sessions=threading.local(), smartlead_limiter=app.RateLimiter(5400)):
                    processor = app.SmartLeadProcessor('local-test-only', logging.getLogger('http-integration'), 'local.log')
                    processor.run_full_process()
                    self.assertEqual(processor.execution_stats['leads_deleted_success'], 2)
                    state = json.loads(Path('run-state.json').read_text())
                    self.assertTrue(Path(state['backup_file']).is_file())
            finally:
                os.chdir(original)
        self.assertEqual(sorted(deleted), ['/campaigns/101/leads/111', '/campaigns/101/leads/333'])
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['to'], ['recipient@example.com'])
        self.assertTrue(any(item['filename'].startswith('leads_deletion_backup_') for item in messages[0]['attachments']))


if __name__ == '__main__':
    unittest.main()
