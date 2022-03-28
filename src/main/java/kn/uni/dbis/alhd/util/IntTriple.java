package kn.uni.dbis.alhd.util;

public final class IntTriple implements Comparable<IntTriple> {
	private final int a;
	private final int b;
	private final int c;
	
	public IntTriple(final int a, final int b, final int c) {
		this.a = a;
		this.b = b;
		this.c = c;
	}

	public int getFirst() {
		return this.a;
	}

	public int getSecond() {
		return this.b;
	}

	public int getThird() {
		return this.c;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof IntTriple)) {
			return false;
		}
		final IntTriple that = (IntTriple) obj;
		return this.a == that.a && this.b == that.b
				&& this.c == that.c;
	}

	@Override
	public int hashCode() {
		long hashCode = this.a;
		hashCode = 1181783497276652981L * hashCode + this.b;
		hashCode = 1181783497276652981L * hashCode + this.c;
		return (int) (hashCode ^ (hashCode >>> 32L));
	}

	@Override
	public int compareTo(final IntTriple that) {
		final int cmpFrom = Integer.compare(this.a, that.a);
		if (cmpFrom != 0) {
			return cmpFrom;
		}
		final int cmpEdge = Integer.compare(this.b, that.b);
		if (cmpEdge != 0) {
			return cmpEdge;
		}
		return Integer.compare(this.c, that.c);
	}

	@Override
	public String toString() {
		return "IntTriple[" + this.a + ", " + this.b + ", " + this.c + "]";
	}
}
