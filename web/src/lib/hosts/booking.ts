/**
 * How the scheduler's overcommitted memory ledger saw a host, for its card.
 *
 * With memory overcommitted, the scheduler does not place against the runners'
 * whole sizes and the host's allocatable memory: it places against what the
 * runners are booked for (by job history under `scheduler.memory_booking`)
 * and the machine's ceiling, shared by every agent on one machine
 * (`scheduler.machine_label`). A card that showed the typed sizes against the
 * allocatable memory would show a host "160 GB of 54 GB committed" while the
 * scheduler sees room, so where the ledger is the one that decides, the
 * Memory row is the ledger's.
 */
import type { Host } from '../api/types';
import { formatMegabytes } from '../format';

type Booking = NonNullable<Host['memory_booking']>;

/** The Memory row of the Committed block, from the ledger, or null where the ledger does not decide. */
export function bookedMemoryRow(
  booking: Booking | null | undefined,
  self?: string,
): { label: string; used: number; total: number; text: string; hint: string } | null {
  if (!booking || !booking.overcommitted || !booking.machine_ceiling_mb) return null;
  const used = Math.max(booking.machine_booked_memory_mb, 0);
  const total = booking.machine_ceiling_mb;
  return {
    label: 'Memory',
    used,
    total,
    text: `${formatMegabytes(used)} of ${formatMegabytes(total)} booked`,
    hint: bookingHint(booking, self),
  };
}

/**
 * "By job history (192 GB at full size) · shared with ci-nl-01-trusted on machine ci-nl-01".
 */
export function bookingHint(booking: Booking, self?: string): string {
  const parts: string[] = [];
  if (booking.mode === 'history') {
    parts.push(
      booking.machine_typed_memory_mb > booking.machine_booked_memory_mb
        ? `By job history (${formatMegabytes(booking.machine_typed_memory_mb)} at full size)`
        : 'By job history; every runner at its full size',
    );
  } else {
    parts.push('At full size');
  }
  const others = (booking.machine_hosts ?? []).filter((name) => name !== self);
  if (booking.machine && others.length > 0) {
    parts.push(`shared with ${others.join(', ')} on machine ${booking.machine}`);
  }
  return parts.join(' · ');
}
