package com.moveinsync.scheduler.app.api.dto;

import java.util.List;

/**
 * One page of a list. {@code hasMore} tells whether a next page exists (no total count, which
 * would cost a full scan).
 */
public record PageView<T>(List<T> items, int page, int size, boolean hasMore) {

    /** Builds a page from {@code size + 1} fetched rows. */
    public static <T> PageView<T> of(List<T> fetched, int page, int size) {
        boolean more = fetched.size() > size;
        return new PageView<>(more ? List.copyOf(fetched.subList(0, size)) : List.copyOf(fetched), page, size, more);
    }
}
