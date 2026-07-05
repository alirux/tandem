package com.codingful.tandem.core.port;

import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.ReplayResult;

/** Replay matching rows back to {@code PENDING} for re-delivery (LLD-core §2.6, §8). Honours {@code dryRun}. */
public interface ReplayService {

    /**
     * @param criteria which rows to match, and whether to actually reset them ({@code dryRun})
     * @return the match/replay counts (§8)
     */
    ReplayResult replay(ReplayCriteria criteria);
}
