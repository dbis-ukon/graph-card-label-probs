package kn.uni.dbis.alhd.util;

public final class IntPair implements Comparable<IntPair> {
	private final int a;
	private final int b;
	
	public IntPair(final int a, final int b) {
		this.a = a;
		this.b = b;
	}

	public int getFirst() {
		return this.a;
	}

	public int getSecond() {
		return this.b;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof IntPair)) {
			return false;
		}
		final IntPair that = (IntPair) obj;
		return this.a == that.a && this.b == that.b;
	}

	@Override
	public int hashCode() {
		final long hashCode = 1181783497276652981L * this.a + this.b;
		return (int) (hashCode ^ (hashCode >>> 32L));
	}

	@Override
	public int compareTo(final IntPair that) {
		final int cmpA = Integer.compare(this.a, that.a);
		return cmpA != 0 ? cmpA : Integer.compare(this.b, that.b);
	}

	@Override
	public String toString() {
		return "IntPair[" + this.a + ", " + this.b + "]";
	}
}
