package org.apache.solr.encryption.matcher;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * A Hamcrest matcher that retries in loop a condition and fails only after a timeout expired.
 */
public class EventuallyMatcher<T> extends TypeSafeMatcher<Supplier<T>> {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(5000);
    private static final long SLEEP_TIME_MS = 500;

    private final Matcher<T> matcher;
    private final Duration timeout;

    private EventuallyMatcher(Matcher<T> matcher) {
        this(matcher, DEFAULT_TIMEOUT);
    }

    private EventuallyMatcher(Matcher<T> matcher, Duration timeout) {
        this.matcher = matcher;
        this.timeout = timeout;
    }

    public static <T> EventuallyMatcher<T> eventually(Matcher<T> matcher) {
        return new EventuallyMatcher<T>(matcher);
    }

    public static <T> EventuallyMatcher<T> eventually(Matcher<T> matcher, Duration timeout) {
        return new EventuallyMatcher<T>(matcher, timeout);
    }

    @Override
    protected boolean matchesSafely(Supplier<T> item) {
        Instant start = Instant.now();
        while (Duration.between(start, Instant.now()).compareTo(timeout) < 0) {
            T val = item.get();
            if (val != null && matcher.matches(val)) {
                return true;
            }
            try {
                Thread.sleep(SLEEP_TIME_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return false;
    }

    @Override
    public void describeTo(Description description) {
        description.appendDescriptionOf(matcher);
    }

    @Override
    public void describeMismatchSafely(Supplier<T> item, Description mismatchDescription) {
        mismatchDescription.appendText(item.get().toString());
    }
}
