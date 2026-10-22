import csv
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import drain


class DrainTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        original = os.getcwd()
        os.chdir(self.folder.name)
        self.addCleanup(os.chdir, original)
        self.old = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
        self.campaigns = [dict(id=101, name='First', client_id=7, status='PAUSED', created_at=self.old),
                          dict(id=102, name='Second', client_id=7, status='COMPLETED', created_at=self.old)]
        self.rows = {'101': [{'id': '1', 'reply_count': '0', 'note': 'quoted\nnewline'},
                             {'id': '2', 'reply_count': '1', 'note': 'replied'},
                             {'id': '3', 'reply_count': '0', 'note': 'last'}],
                     '102': [{'id': '4', 'reply_count': '0', 'note': 'other campaign'}]}

        def export(key, campaign_id, logger):
            target = Path(f'exports/leads_campaign_{campaign_id}.csv')
            target.parent.mkdir(exist_ok=True)
            with target.open('w', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=['id', 'reply_count', 'note'])
                writer.writeheader()
                writer.writerows(self.rows[str(campaign_id)])
            return str(target)

        for mock in [patch.object(drain.app, 'fetch_all_campaigns', side_effect=lambda *args: self.campaigns),
                     patch.object(drain.app, 'export_leads_csv', side_effect=export),
                     patch.object(drain.app, 'send_email', return_value=True),
                     patch.object(drain.app, 'EXCLUDE_CLIENT_IDS', [1598]),
                     patch.object(drain.app.requests.sessions.Session, 'request',
                                  side_effect=AssertionError('External HTTP forbidden in tests'))]:
            mock.start()
            self.addCleanup(mock.stop)

    def test_plan_preserves_policy_and_resume_cursor(self):
        self.campaigns += [dict(self.campaigns[0], id=103, client_id=1598),
                           dict(self.campaigns[0], id=104, status='ACTIVE'),
                           dict(self.campaigns[0], id=105, created_at=datetime.now(timezone.utc).isoformat())]
        state = drain.plan(resume_after=101)
        self.assertEqual([row['id'] for row in state['campaigns']], [102])

    def test_batch_limits_memory_but_does_not_limit_total_cleanup(self):
        drain.plan()
        deleted = []
        with patch.object(drain.app.SmartLeadProcessor, 'delete_single_lead',
                          side_effect=lambda campaign, lead: deleted.append((campaign, lead)) or True):
            first = drain.prepare(batch_size=2)
            self.assertEqual(first['rows'], 2)
            drain.execute(artifact_id=123)
            second = drain.prepare(batch_size=2)
            self.assertEqual(second['rows'], 1)
            drain.execute(artifact_id=124)
        self.assertEqual(sorted(deleted), [('101', '1'), ('101', '3'), ('102', '4')])
        self.assertEqual(drain.load()['stats']['leads_deleted_success'], 3)
        self.assertEqual(drain.load()['confirmed_after'], 102)
        self.assertTrue(drain.load()['done'])

    def test_no_deletion_without_uploaded_artifact(self):
        drain.plan()
        drain.prepare()
        with patch.object(drain.app.SmartLeadProcessor, 'delete_leads') as delete:
            with self.assertRaisesRegex(ValueError, 'durable artifact'):
                drain.execute(artifact_id=0)
        delete.assert_not_called()

    def test_changed_backup_is_rejected(self):
        drain.plan()
        batch = drain.prepare()
        with Path(batch['backup_file']).open('a') as handle:
            handle.write('tampered')
        with self.assertRaisesRegex(ValueError, 'checksum'):
            drain.execute(artifact_id=1)

    def test_dry_run_cannot_delete_and_samples_only_eligible_campaigns(self):
        with self.assertRaises(ValueError):
            drain.plan(sample=1)
        drain.plan(dry_run=True, sample=1)
        drain.prepare()
        with patch.object(drain.app.SmartLeadProcessor, 'delete_leads') as delete:
            drain.execute(artifact_id=1)
        delete.assert_not_called()
        self.assertEqual(drain.load()['stats']['leads_deleted_success'], 0)
        self.assertEqual(drain.load()['stats']['leads_backed_up'], 2)

    def test_dry_run_manifest_cannot_be_switched_to_live(self):
        drain.plan(dry_run=True, sample=1)
        drain.prepare()
        state = drain.load()
        state['dry_run'] = False
        drain.save(drain.STATE, state)
        with self.assertRaisesRegex(ValueError, 'dry-run backup'):
            drain.execute(artifact_id=1)

    def test_partial_batch_deadline_does_not_skip_campaigns_on_resume(self):
        drain.plan()
        drain.prepare()
        with patch.object(drain.app.SmartLeadProcessor, 'delete_leads', return_value=False):
            result = drain.execute(artifact_id=1)
        self.assertFalse(result['finished'])
        self.assertEqual(drain.load()['confirmed_after'], 0)

    def test_policy_metadata_rejects_recent_or_excluded_campaign(self):
        state = drain.plan()
        batch = drain.prepare()
        frame = drain.app.pd.read_csv(batch['backup_file'])
        frame.loc[0, 'Campaign Created At'] = datetime.now(timezone.utc).isoformat()
        with self.assertRaisesRegex(ValueError, 'approved policy'):
            drain.validate_backup(frame, state)
        frame.loc[0, 'Campaign Created At'] = self.old
        frame.loc[0, 'Client ID'] = 1598
        with self.assertRaisesRegex(ValueError, 'approved policy'):
            drain.validate_backup(frame, state)

    def test_report_uses_this_run_counts_and_continuation_status(self):
        drain.plan()
        drain.report('continuing')
        self.assertEqual(json.loads(Path('summary.json').read_text())['status'], 'continuing')
        args = drain.app.send_email.call_args.args
        self.assertIn('No total lead target', args[1])
        self.assertIn('continuation run is queued', args[1])

    def test_deletion_deadline_does_not_start_another_request(self):
        processor = drain.app.SmartLeadProcessor('test-only', drain.LOG, 'test.log')
        frame = drain.app.pd.DataFrame([{'Campaign ID': '101', 'id': '1', 'reply_count': 0}])
        with patch.object(processor, 'delete_single_lead') as delete:
            self.assertFalse(processor.delete_leads(frame, deadline=0))
        delete.assert_not_called()

    def test_failed_deletion_stops_before_the_next_worker_batch(self):
        processor = drain.app.SmartLeadProcessor('test-only', drain.LOG, 'test.log')
        frame = drain.app.pd.DataFrame([{'Campaign ID': '101', 'id': str(i), 'reply_count': 0}
                                       for i in range(1, 4)])
        with patch.object(drain.app, 'DELETE_WORKERS', 1), \
             patch.object(processor, 'delete_single_lead', return_value=False) as delete:
            with self.assertRaisesRegex(RuntimeError, 'stopping this batch'):
                processor.delete_leads(frame, stop_on_failure=True)
        self.assertEqual(delete.call_count, 1)
        self.assertEqual(processor.execution_stats['leads_deleted_failed'], 1)


if __name__ == '__main__':
    unittest.main()
