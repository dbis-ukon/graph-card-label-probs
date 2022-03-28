package kn.uni.dbis.alhd.util;

import java.util.Arrays;

public final class UnionFind {

	public static boolean union(final int[] unionFind, final int a, final int b) {
		final int ra = find(unionFind, a);
		final int rb = find(unionFind, b);
		if (ra == rb) {
			return false;
		}
		if (unionFind[ra] < unionFind[rb]) {
			// `a`'s tree is bigger
			unionFind[ra] += unionFind[rb];
			unionFind[rb] = ra;
		} else {
			unionFind[rb] += unionFind[ra];
			unionFind[ra] = rb;
		}
		return true;
	}

	public static int find(final int[] unionFind, final int a) {
		if (unionFind[a] < 0) {
			return a;
		}
		final int ra = find(unionFind, unionFind[a]);
		unionFind[a] = ra;
		return ra;
	}

	public static int[] create(final int n) {
		final int[] sets = new int[n];
		Arrays.fill(sets, -1);
		return sets;
	}
}
