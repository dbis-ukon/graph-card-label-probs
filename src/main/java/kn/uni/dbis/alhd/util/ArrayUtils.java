package kn.uni.dbis.alhd.util;

import java.util.*;
import java.util.stream.Stream;

public class ArrayUtils {
	public static <T> void shuffle(final Random rng, final T[] array, final int from, final int to) {
		final int n = to - from;
		for (int i = 1; i < n; i++) {
			final int j = rng.nextInt(i + 1);
			if (i != j) {
				final T temp = array[from + i];
				array[from + i] = array[from + j];
				array[from + j] = temp;
			}
		}
	}

	public static void shuffle(final Random rng, final int[] array, final int from, final int to) {
		final int n = to - from;
		for (int i = 1; i < n; i++) {
			final int j = rng.nextInt(i + 1);
			if (i != j) {
				final int temp = array[from + i];
				array[from + i] = array[from + j];
				array[from + j] = temp;
			}
		}
	}

	public static int[] sample(final Random rng, final int n, final int k) {
		final int[] reservoir = new int[Math.min(n, k)];
		for (int i = 0; i < n; i++) {
			final int j = i < k ? i : rng.nextInt(i + 1);
			if (j < k) {
				reservoir[j] = i;
			}
		}
		Arrays.sort(reservoir);
		return reservoir;
	}

	public static <T> List<T> sample(final Random rng, final int k, final Stream<T> candidates) {
		final List<T> reservoir = new ArrayList<>();
		final Iterator<T> iter = candidates.iterator();
		for (int i = 0; i < Integer.MAX_VALUE && iter.hasNext(); i++) {
			final T candidate = iter.next();
			if (i < k) {
				reservoir.add(candidate);
			} else {
				final int j = rng.nextInt(i + 1);
				if (j < k) {
					reservoir.set(j, candidate);
				}
			}
		}
		return reservoir;
	}

	public static int[] sample(final Random rng, final int k, final int[] candidates) {
		final int[] reservoir = new int[Math.min(candidates.length, k)];
		for (int i = 0; i < candidates.length; i++) {
			final int j = i < k ? i : rng.nextInt(i + 1);
			if (j < k) {
				reservoir[j] = candidates[i];
			}
		}
		return reservoir;
	}
}
