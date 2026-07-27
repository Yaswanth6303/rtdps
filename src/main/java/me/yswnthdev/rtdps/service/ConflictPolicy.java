package me.yswnthdev.rtdps.service;

/**
 * Write semantics for the import job when the source row's {@code id} already
 * exists in {@code customers}. Chosen per-request via the
 * {@code ?conflictPolicy=} query parameter on {@code POST /jobs/importCustomers}.
 *
 * <ul>
 *   <li>{@link #SKIP} — {@code ON CONFLICT (id) DO NOTHING}. Idempotent
 *       re-imports never mutate data; the conflict count is surfaced on the
 *       {@code JobTracker} so the caller knows how many rows were dropped.</li>
 *   <li>{@link #UPDATE} — {@code ON CONFLICT (id) DO UPDATE SET …}. Last
 *       writer wins. The writer pre-counts existing IDs in each chunk so the
 *       {@code insertedCount}/{@code updatedCount} split is accurate, not a
 *       guess.</li>
 * </ul>
 */
public enum ConflictPolicy {
    SKIP,
    UPDATE;

    public static ConflictPolicy parseOrDefault(String name) {
        if (name == null || name.isBlank()) return SKIP;
        try {
            return ConflictPolicy.valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return SKIP;
        }
    }
}
