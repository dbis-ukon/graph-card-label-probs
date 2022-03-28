package kn.uni.dbis.alhd.graphlets;

public enum GraphletShape {
	CHAIN,
	TREE,
	STAR,
	CIRCLE,
	PETAL,
	FLOWER,
	DENSE;

	public boolean isCyclic() {
		return this.ordinal() >= CIRCLE.ordinal();
	}
}
