import { test } from 'node:test';
import assert from 'node:assert/strict';
import { bookedMemoryRow, bookingHint } from '../src/lib/hosts/booking.ts';

// With memory overcommitted the scheduler places against what runners are
// booked for and the machine's ceiling, so the card's Memory row is that
// ledger, and says how runners were booked and which agents share it.

const shared = {
  mode: 'history' as const,
  booked_memory_mb: 70_000,
  typed_memory_mb: 163_840,
  by_history: 4,
  machine: 'ci-nl-01',
  machine_hosts: ['ci-nl-01-pr', 'ci-nl-01-trusted'],
  machine_booked_memory_mb: 98_304,
  machine_typed_memory_mb: 196_608,
  machine_ceiling_mb: 167_232,
  overcommitted: true,
};

test('the Memory row is the booking against the machine ceiling', () => {
  const row = bookedMemoryRow(shared, 'ci-nl-01-pr');
  assert.ok(row);
  assert.equal(row.used, 98_304);
  assert.equal(row.total, 167_232);
  assert.equal(row.text, '96 GB of 163 GB booked');
  assert.match(row.hint, /shared with ci-nl-01-trusted on machine ci-nl-01$/);
});

test('the hint says the runners were booked by history and who shares the machine', () => {
  assert.equal(
    bookingHint(shared, 'ci-nl-01-pr'),
    'By job history (192 GB at full size) · shared with ci-nl-01-trusted on machine ci-nl-01',
  );
});

test('a host whose ledger does not decide keeps the plain row', () => {
  assert.equal(bookedMemoryRow({ ...shared, overcommitted: false }), null);
  assert.equal(bookedMemoryRow(undefined), null);
});
