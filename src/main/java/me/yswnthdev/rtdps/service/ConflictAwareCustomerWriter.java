package me.yswnthdev.rtdps.service;

import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.entity.Customer;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * Custom writer that distinguishes <b>inserted</b>, <b>updated</b> and
 * silently-skipped <b>conflicting</b> rows — fixing the lie that
 * {@code JdbcBatchItemWriter} + {@code ON CONFLICT DO NOTHING} told us, where
 * conflicts counted as writes.
 * <p>
 * Counters use {@link LongAdder} because a multi-threaded step shares this
 * single {@code @StepScope}-d instance across worker threads.
 * <p>
 * Counts are pushed to the {@code StepExecution} context in
 * {@link #afterStep(StepExecution)} so the {@code JobCompletionListener} can
 * roll them up into the {@code JobTracker} row.
 */
@Slf4j
public class ConflictAwareCustomerWriter implements ItemWriter<Customer>, StepExecutionListener {

    public static final String CTX_INSERTED  = "rtdps.inserted";
    public static final String CTX_UPDATED   = "rtdps.updated";
    public static final String CTX_CONFLICTS = "rtdps.conflicts";

    private static final String INSERT_BASE = """
            INSERT INTO customers (id, first_name, last_name, email, gender, contact_no, country, dob)
            VALUES (:id, :firstName, :lastName, :email, :gender, :contactNo, :country, :dob)
            """;

    static final String SQL_SKIP = INSERT_BASE + "ON CONFLICT (id) DO NOTHING";

    static final String SQL_UPDATE = INSERT_BASE + """
            ON CONFLICT (id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name  = EXCLUDED.last_name,
                email      = EXCLUDED.email,
                gender     = EXCLUDED.gender,
                contact_no = EXCLUDED.contact_no,
                country    = EXCLUDED.country,
                dob        = EXCLUDED.dob
            """;

    private static final String SQL_EXISTING_IDS = "SELECT id FROM customers WHERE id IN (:ids)";

    private final NamedParameterJdbcTemplate jdbc;
    private final ConflictPolicy policy;

    private final LongAdder inserted = new LongAdder();
    private final LongAdder updated = new LongAdder();
    private final LongAdder conflicted = new LongAdder();

    public ConflictAwareCustomerWriter(NamedParameterJdbcTemplate jdbc, ConflictPolicy policy) {
        this.jdbc = jdbc;
        this.policy = policy;
    }

    @Override
    public void write(Chunk<? extends Customer> chunk) {
        if (chunk.isEmpty()) return;

        List<? extends Customer> items = chunk.getItems();
        SqlParameterSource[] params = items.stream()
                .map(BeanPropertySqlParameterSource::new)
                .toArray(SqlParameterSource[]::new);

        if (policy == ConflictPolicy.UPDATE) {
            // One extra SELECT per chunk so we can report an accurate inserted/updated split
            // instead of guessing. Cheap at chunk sizes in the tens.
            List<Integer> ids = items.stream().map(Customer::getId).toList();
            List<Integer> existing = jdbc.queryForList(
                    SQL_EXISTING_IDS,
                    new MapSqlParameterSource("ids", ids),
                    Integer.class);
            int existingCount = existing.size();
            int newCount = items.size() - existingCount;

            jdbc.batchUpdate(SQL_UPDATE, params);
            inserted.add(newCount);
            updated.add(existingCount);
        } else {
            // SKIP path: batchUpdate returns 1 per inserted row, 0 per conflicted no-op.
            int[] counts = jdbc.batchUpdate(SQL_SKIP, params);
            int written = 0;
            for (int c : counts) {
                if (c == Statement.SUCCESS_NO_INFO) written += 1;   // driver didn't tell us; assume insert
                else if (c >= 0)                    written += c;
            }
            inserted.add(written);
            conflicted.add(items.size() - written);
        }
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        inserted.reset();
        updated.reset();
        conflicted.reset();
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        long i = inserted.sum(), u = updated.sum(), c = conflicted.sum();
        stepExecution.getExecutionContext().putLong(CTX_INSERTED,  i);
        stepExecution.getExecutionContext().putLong(CTX_UPDATED,   u);
        stepExecution.getExecutionContext().putLong(CTX_CONFLICTS, c);
        log.info("[ConflictAwareCustomerWriter] policy={} inserted={} updated={} conflicted={}",
                policy, i, u, c);
        return null;                  // leave the step's natural exit status
    }
}
