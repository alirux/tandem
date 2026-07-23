package com.codingful.tandem.core.port;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OutboxStoreDefaultTest {

    @Test
    void GIVEN_multiple_ids_WHEN_marked_done_in_batch_THEN_markDone_is_called_once_per_id_in_order() {
        List<Long> markedDone = new ArrayList<>();

        OutboxStore store = new OutboxStore() {
            @Override
            public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
                return List.of();
            }

            @Override
            public void markDone(long id) {
                markedDone.add(id);
            }

            @Override
            public void markForRetry(long id, String error, Duration retryDelay) {
            }

            @Override
            public void markFailed(long id, String error) {
            }

            @Override
            public int reclaimExpiredLeases() {
                return 0;
            }

            @Override
            public int cleanup(Instant doneBefore, int batchSize) {
                return 0;
            }
        };

        store.markDoneBatch(List.of(10L, 20L, 30L));

        assertThat(markedDone).containsExactly(10L, 20L, 30L);
    }

    @Test
    void GIVEN_an_empty_batch_WHEN_marked_done_THEN_markDone_is_never_called() {
        List<Long> markedDone = new ArrayList<>();

        OutboxStore store = new OutboxStore() {
            @Override
            public List<OutboxRecord> claimBatch(Set<Integer> b, String w, Duration l, int s) { return List.of(); }
            @Override public void markDone(long id) { markedDone.add(id); }
            @Override public void markForRetry(long id, String error, Duration retryDelay) {}
            @Override public void markFailed(long id, String error) {}
            @Override public int reclaimExpiredLeases() { return 0; }
            @Override public int cleanup(Instant doneBefore, int batchSize) { return 0; }
        };

        store.markDoneBatch(List.of());

        assertThat(markedDone).isEmpty();
    }
}
