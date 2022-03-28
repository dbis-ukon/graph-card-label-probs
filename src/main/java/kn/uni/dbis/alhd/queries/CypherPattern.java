package kn.uni.dbis.alhd.queries;

import kn.uni.dbis.alhd.estimator.LabelDistribution;
import kn.uni.dbis.alhd.graphlets.GraphletShape;
import kn.uni.dbis.alhd.statistics.GraphStatistics;
import kn.uni.dbis.alhd.util.ArrayUtils;
import kn.uni.dbis.alhd.util.UnionFind;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CypherPattern implements Cloneable {
	private final Map<String, Set<String>> nodeVars = new HashMap<>();
	private final List<Relationship> relationships = new ArrayList<>();

	public CypherPattern() {
	}

	public CypherPattern(final String var, final String... labels) {
		this.nodeVars.put(var, new TreeSet<>(Arrays.asList(labels)));
	}

	public CypherPattern(final Map<String, Set<String>> newNodes, final List<Relationship> relationships) {
		this.nodeVars.putAll(newNodes);
		this.relationships.addAll(relationships);
	}

	public CypherPattern var(final String var, final String... labels) {
		final Set<String> labelSet = this.nodeVars.computeIfAbsent(var, v -> new TreeSet<>());
		labelSet.addAll(Arrays.asList(labels));
		return this;
	}

	public String rel(final String var1, final String type, final Direction dir, final String var2) {
		this.nodeVars.computeIfAbsent(var1, v -> new TreeSet<>());
		this.nodeVars.computeIfAbsent(var2, v -> new TreeSet<>());
		Relationship rel;
		switch (dir) {
		case BOTH:
			rel = new Relationship(var1, var2, type, false);
			break;
		case INCOMING:
			rel = new Relationship(var2, var1, type, true);
			break;
		default:
		case OUTGOING:
			rel = new Relationship(var1, var2, type, true);
			break;
		}
		this.relationships.add(rel);
		return rel.name();
	}

	public String rel(final String var1, final String var2) {
		return this.rel(var1, null, Direction.OUTGOING, var2);
	}

	public String rel(final String var1, final String type, final String var2) {
		return this.rel(var1, type, Direction.OUTGOING, var2);
	}

	public int numNodeVars() {
		return this.nodeVars.size();
	}

	public int numRelationships() {
		return this.relationships.size();
	}

	public int numNodeLabels() {
		return (int) this.nodeVars.values().stream().mapToLong(Collection::size).sum();
	}

	public int numEdgeTypes() {
		return (int) this.relationships.stream().filter(r -> r.type != null).count();
	}

	public int numDirected() {
		return (int) this.relationships.stream().filter(r -> r.directed).count();
	}

	public Optional<String[][]> toTriples(final boolean rdfType) {
		int dummy = 0;
		final List<String[]> triples = new ArrayList<>();
		for (final Entry<String, Set<String>> e : this.nodeVars.entrySet()) {
			for (final String label : e.getValue()) {
				if (rdfType) {
					triples.add(new String[] { "?" + e.getKey(), "rdf:type", label });
				} else {
					triples.add(new String[] { "?" + e.getKey(), "L:" + label, "l" });
				}
			}
		}
		for (final Relationship rel : this.relationships) {
			if (!rel.directed) {
				return Optional.empty();
			}
			final String pred = rel.type == null ? "?dummy" + dummy++ : "T:" + rel.type;
			triples.add(new String[] { "?" + rel.source, pred, "?" + rel.target });
		}
		triples.sort(Comparator.comparing(t -> t[1]));
		return Optional.of(triples.toArray(String[][]::new));
	}

	public Optional<String[]> asSimplePath() {
		if (this.relationships.size() != this.nodeVars.size() - 1) {
			return Optional.empty();
		}
		final Map<String, Relationship> left = new HashMap<>();
		final Set<String> right = new HashSet<>();
		for (final Relationship rel : this.relationships) {
			if (!rel.directed || left.containsKey(rel.source) || right.contains(rel.target) || rel.type == null) {
				return Optional.empty();
			}
			left.put(rel.source, rel);
			right.add(rel.target);
		}
		final Set<String> starts = new HashSet<>(this.nodeVars.keySet());
		starts.removeAll(right);
		
		final List<String> path = new ArrayList<>();
		String node = starts.stream().findFirst().get();
		for (;;) {
			if (!this.nodeVars.get(node).isEmpty()) {
				return Optional.empty();
			}
			final Relationship rel = left.get(node);
			if (rel == null) {
				break;
			}
			path.add(rel.type);
			node = rel.target;
		}
		return Optional.of(path.toArray(String[]::new));
	}

	public CypherPattern onlyNodeLabelsOn(final String... nodeVars) {
		final Set<String> whiteList = new HashSet<>(Arrays.asList(nodeVars));
		final Map<String, Set<String>> newNodes = new HashMap<>();
		for (final Entry<String, Set<String>> e : this.nodeVars.entrySet()) {
			final String v = e.getKey();
			newNodes.put(v, new HashSet<>(whiteList.contains(v) ? e.getValue() : Collections.emptySet()));
		}
		return new CypherPattern(newNodes, this.relationships);
	}

	public Optional<String[][]> asLabledPath() {
		if (this.relationships.size() != this.nodeVars.size() - 1) {
			return Optional.empty();
		}
		final Map<String, Relationship> left = new HashMap<>();
		final Set<String> right = new HashSet<>();
		for (final Relationship rel : this.relationships) {
			if (!rel.directed || left.containsKey(rel.source) || right.contains(rel.target)) {
				return Optional.empty();
			}
			left.put(rel.source, rel);
			right.add(rel.target);
		}
		final Set<String> starts = new HashSet<>(this.nodeVars.keySet());
		starts.removeAll(right);

		final List<String[]> path = new ArrayList<>();
		String node = starts.stream().findFirst().get();
		for (;;) {
			path.add(this.nodeVars.get(node).toArray(String[]::new));
			final Relationship rel = left.get(node);
			if (rel == null) {
				break;
			}
			path.add(rel.type == null ? new String[0] : new String[] { rel.type });
			node = rel.target;
		}
		return Optional.of(path.toArray(String[][]::new));
	}

	public Optional<Object[]> asMixedPath() {
		if (this.relationships.size() != this.nodeVars.size() - 1) {
			return Optional.empty();
		}
		final Map<String, List<Relationship>> neighbors = new HashMap<>();
		for (final Relationship rel : this.relationships) {
			if (!rel.directed) {
				return Optional.empty();
			}
			final List<Relationship> left = neighbors.computeIfAbsent(rel.source, k -> new ArrayList<>());
			final List<Relationship> right = neighbors.computeIfAbsent(rel.target, k -> new ArrayList<>());
			if (left.size() > 1 || right.size() > 1) {
				return Optional.empty();
			}
			left.add(rel);
			right.add(rel);
		}
		final List<String> starts = neighbors.entrySet().stream()
				.filter(e -> e.getValue().size() == 1)
				.map(Entry::getKey)
				.collect(Collectors.toList());
		if (starts.size() != 2) {
			return Optional.empty();
		}

		final String[][] nodes = new String[this.nodeVars.size()][];
		final String[] edges = new String[this.nodeVars.size() - 1];
		final boolean[] reverses = new boolean[edges.length];
		String node = starts.get(0);
		nodes[0] = this.nodeVars.get(node).toArray(String[]::new);
		final Set<Relationship> seen = new HashSet<>();
		for (int i = 0;; i++) {
			final Optional<Relationship> next = neighbors.get(node).stream().filter(rel -> !seen.contains(rel)).findFirst();
			if (next.isEmpty()) {
				break;
			}
			final Relationship rel = next.get();
			seen.add(rel);
			final boolean reverse = !rel.source.equals(node);
			reverses[i] = reverse;
			edges[i] = rel.type;
			node = reverse ? rel.source : rel.target;
			nodes[i + 1] = this.nodeVars.get(node).toArray(String[]::new);
		}
		return Optional.of(new Object[] { nodes, edges, reverses });
	}

	@Override
	@Deprecated
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		toString(null, part -> {
			if (sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(part);
		});
		return sb.toString();
	}

	public final String toMatchClauses(final Map<Set<String>, String> edgeNames, final boolean homomorphism) {
		final StringBuilder sb = new StringBuilder();
		toString(edgeNames, part -> {
			sb.append(sb.length() == 0 ? "MATCH " : homomorphism ? " MATCH " : ", ").append(part);
		});
		return sb.toString();
	}

	public final void toString(final Map<Set<String>, String> edgeNames, final Consumer<String> out) {
		final StringBuilder sb = new StringBuilder();
		final Set<String> seen = new HashSet<>();
		final boolean namedEdges = edgeNames != null;
		int i = 0;
		for (final Relationship rel : this.relationships) {
			sb.setLength(0);
			sb.append("(").append(rel.source);
			if (seen.add(rel.source)) {
				for (final String label : this.nodeVars.get(rel.source)) {
					sb.append(":").append(label);
				}
			}
			sb.append(")-");
			if (rel.type != null || namedEdges) {
				sb.append('[');
				if (namedEdges) {
					final Set<String> endPoints = new HashSet<>(Arrays.asList(rel.source, rel.target));
					sb.append(edgeNames.containsKey(endPoints) ? edgeNames.get(endPoints) : rel.name());
				}
				if (rel.type != null) {
					sb.append(':').append(rel.type);
				}
				sb.append(']');
			}
			sb.append(rel.directed ? "->(" : "-(").append(rel.target);
			if (seen.add(rel.target)) {
				for (final String label : this.nodeVars.get(rel.target)) {
					sb.append(":").append(label);
				}
			}
			sb.append(")");
			out.accept(sb.toString());
		}
		for (final Entry<String, Set<String>> e : this.nodeVars.entrySet()) {
			if (seen.contains(e.getKey())) {
				continue;
			}
			sb.setLength(0);
			sb.append("(").append(e.getKey());
			for (final String label : e.getValue()) {
				sb.append(":").append(label);
			}
			sb.append(")");
			out.accept(sb.toString());
		}
	}

	/**
	 * Returns a canonical Cypher representation of this pattern.
	 *
	 * @return canonical string representation
	 */
	public final String normalized(final boolean namedEdges) {
		final StringBuilder sb = new StringBuilder();
		final Set<String> remaining = new HashSet<>(this.nodeVars.keySet());
		final List<Relationship> rels = new ArrayList<>(this.relationships);
		Collections.sort(rels);
		for (final Relationship rel : rels) {
			if (sb.length() > 0) {
				sb.append(",");
			}
			nodeVar(sb, remaining, rel.source);
			sb.append("-");
			if (rel.type != null || namedEdges) {
				sb.append('[');
				if (namedEdges) {
					sb.append(rel.name());
				}
				if (rel.type != null) {
					sb.append(':').append(rel.type);
				}
				sb.append(']');
			}
			sb.append(rel.directed ? "->" : "-");
			nodeVar(sb, remaining, rel.target);
		}
		final List<String> rest = new ArrayList<>(remaining);
		Collections.sort(rest);
		for (final String nv : rest) {
			if (sb.length() > 0) {
				sb.append(",");
			}
			nodeVar(sb, remaining, nv);
		}
		return sb.toString();
	}

	private void nodeVar(final StringBuilder sb, final Set<String> remaining, final String name) {
		sb.append('(').append(name);
		if (remaining.remove(name)) {
			final List<String> lbls = new ArrayList<>(this.nodeVars.get(name));
			Collections.sort(lbls);
			for (final String lbl : lbls) {
				sb.append(':').append(lbl);
			}
		}
		sb.append(')');
	}

	public static class Relationship implements Comparable<Relationship> {
		private final String source;
		private final String target;
		private final boolean directed;
		private final String type;
		Relationship(final String source, final String target, final String label, final boolean directed) {
			this.source = source;
			this.target = target;
			this.directed = directed;
			this.type = label;
		}

		public final Relationship redirect(final String newTarget) {
			return new Relationship(this.source, newTarget, this.type, this.directed);
		}

		public final String name() {
			return source.replaceAll("_", "__") + "_" + target.replaceAll("_", "__");
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder("(").append(this.source).append(")-[").append(this.name());
			if (this.type != null) {
				sb.append(":").append(this.type);
			}
			return sb.append("]-").append(this.directed ? ">(" : "(").append(this.target).append(")").toString();
		}

		public String getSource() {
			return this.source;
		}

		public Optional<String> getType() {
			return Optional.ofNullable(this.type);
		}

		public String getTarget() {
			return this.target;
		}

		public boolean isDirected() {
			return this.directed;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof Relationship)) {
				return false;
			}
			final Relationship that = (Relationship) obj;
			return Objects.equals(this.source, that.source) && this.directed == that.directed
					&& Objects.equals(this.type, that.type) && Objects.equals(this.target, that.target);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.source, this.directed, this.type, this.target);
		}

		@Override
		public int compareTo(final Relationship that) {
			if (this == that) {
				return 0;
			}
			final int cSrc = this.source.compareTo(that.source);
			if (cSrc != 0) {
				return cSrc;
			}
			final int cType = this.type != null ? that.type == null ? 1 : this.type.compareTo(that.type)
					                            : that.type == null ? 0 : -1;
			if (cType != 0) {
				return cType;
			}
			if (this.directed ^ that.directed) {
				return this.directed ? 1 : -1;
			}
			return this.target.compareTo(that.target);
		}
	}

	private static final Pattern NODE_VAR = Pattern.compile("\\(([a-zA-Z0-9_]*)((?::[a-zA-Z0-9_]+)*)\\)");
	private static final Pattern REL = Pattern.compile("\\(([a-zA-Z0-9_]*)(?::[^\\)]+)*\\)(<?-)(?:\\[((?::[a-zA-Z0-9_]+)?)\\])?(->?)\\(([a-zA-Z0-9_]*)(?::[^\\)]+)*\\)");

	public static CypherPattern fromCypher(final String in) {
		final CypherPattern pattern = new CypherPattern();
		for (final String part : in.split("\\s*,\\s*")) {
			Matcher matcher = NODE_VAR.matcher(part);
			while (matcher.find()) {
				final String var = matcher.group(1);
				if (var.isEmpty()) {
					throw new IllegalArgumentException("No var name.");
				}
				final String labels = matcher.group(2).replaceFirst("^:", "");
				pattern.var(var, labels.isEmpty() ? new String[0] : labels.split(":"));
			}
			final Matcher relMatcher = REL.matcher(part);
			int start = 0;
			while (relMatcher.find(start)) {
				final String varL = relMatcher.group(1);
				final String relL = relMatcher.group(2);
				final String label = relMatcher.group(3);
				final String relR = relMatcher.group(4);
				final String varR = relMatcher.group(5);
				final Direction dir = relL.startsWith("<") ? Direction.INCOMING
						: relR.endsWith(">") ? Direction.OUTGOING : Direction.BOTH;
				pattern.rel(varL, label != null && label.startsWith(":") ? label.substring(1) : null, dir, varR);
				start = relMatcher.start() + 1;
			}
		}
		return pattern;
	}

	public Map<String, Set<String>> getNodeVars() {
		return Collections.unmodifiableMap(this.nodeVars);
	}

	public List<Relationship> getRelationships() {
		return Collections.unmodifiableList(this.relationships);
	}

	@Override
	public CypherPattern clone() {
		final CypherPattern copy = new CypherPattern();
		for (final Entry<String, Set<String>> e : this.nodeVars.entrySet()) {
			copy.var(e.getKey(), e.getValue().toArray(String[]::new));
		}
		for (Relationship r : this.relationships) {
			copy.rel(r.source, r.type, r.directed ? Direction.OUTGOING : Direction.BOTH, r.target);
		}
		return copy;
	}

	public Optional<GraphletShape> getShape(final boolean detectFlower) {
		final int m = this.numRelationships();
		final int n = this.numNodeVars();
		if (m == 0 || !this.isConnected()) {
			return Optional.empty();
		}

		final Map<String, Integer> degrees = new HashMap<>();
		for (final Relationship r : this.relationships) {
			for (final String nv : Arrays.asList(r.source, r.target)) {
				degrees.merge(nv, 1, Integer::sum);
			}
		}

		final int[] degCounts = new int[m + 1];
		for (final int deg : degrees.values()) {
			degCounts[deg]++;
		}

		if (m == n - 1) {
			// acyclic, identified by 
			return Optional.of(degCounts[1] == 2 ? GraphletShape.CHAIN
					: degCounts[1] == n - 1 ? GraphletShape.STAR : GraphletShape.TREE);
		} else if (m == n && degCounts[2] == n) {
			return Optional.of(GraphletShape.CIRCLE);
		} else if (degCounts[1] == 0 && degCounts[2] == n - 2) {
			return Optional.of(GraphletShape.PETAL);
		} else if (detectFlower && this.isFlower()) {
			return Optional.of(GraphletShape.FLOWER);
		} else {
			return Optional.of(GraphletShape.DENSE);
		}
	}

	public boolean isFlower() {
		for (final String center : this.nodeVars.keySet()) {
			if (this.isCenteredFlower(center)) {
				return true;
			}
		}
		return false;
	}

	private boolean isCenteredFlower(String center) {
		final Map<String, List<Relationship>> n2e = new HashMap<>();
		for (final Relationship r : this.relationships) {
			n2e.computeIfAbsent(r.source, k -> new ArrayList<>()).add(r);
			n2e.computeIfAbsent(r.target, k -> new ArrayList<>()).add(r);
		}
		final Map<String, Integer> n2p = new HashMap<>();
		final String[] nodes = this.nodeVars.keySet().toArray(String[]::new);
		for (int i = 0; i < nodes.length; i++) {
			n2p.put(nodes[i], i);
		}

		final int[] sets = UnionFind.create(nodes.length);
		for (final Relationship r : this.relationships) {
			if (!(r.source.equals(center) || r.target.equals(center))) {
				final int src = n2p.get(r.source);
				final int trg = n2p.get(r.target);
				UnionFind.union(sets, src, trg);
			}
		}
		final Map<Integer, List<Relationship>> subgraphs = new HashMap<>();
		for (final Relationship r : this.relationships) {
			if (!r.source.equals(center)) {
				final int set = UnionFind.find(sets, n2p.get(r.source));
				subgraphs.computeIfAbsent(set, k -> new ArrayList<>()).add(r);
			} else if (!r.target.equals(center)) {
				final int set = UnionFind.find(sets, n2p.get(r.target));
				subgraphs.computeIfAbsent(set, k -> new ArrayList<>()).add(r);
			}
		}

		for (final List<Relationship> sub : subgraphs.values()) {
			final Set<String> nvs = sub.stream()
					.flatMap(r -> Stream.of(r.source, r.target))
					.collect(Collectors.toSet());
			final Map<String, Set<String>> lbls = new HashMap<>();
			for (final String nv : nvs) {
				lbls.put(nv, new HashSet<>(this.nodeVars.get(nv)));
			}
			if (new CypherPattern(lbls, sub).getShape(false).get() == GraphletShape.DENSE) {
				return false;
			}
		}
		return true;
	}

	public final boolean isConnected() {
		final int n = this.numNodeVars();
		final int m = this.numRelationships();
		int required = n - 1;
		int remaining = m;
		if (remaining < required) {
			return false;
		}

		// assign consecutive IDs to nodes
		final Map<String, Integer> n2p = new HashMap<>();
		final String[] nodes = this.nodeVars.keySet().toArray(String[]::new);
		for (int i = 0; i < nodes.length; i++) {
			n2p.put(nodes[i], i);
		}

		// find a spanning tree
		final int[] sets = UnionFind.create(nodes.length);
		while (remaining > 0) {
			final Relationship e = this.relationships.get(m - remaining);
			remaining--;
			final int src = n2p.get(e.source);
			final int trg = n2p.get(e.target);
			if (UnionFind.union(sets, src, trg)) {
				if (--required == 0) {
					// spanning tree complete
					return true;
				}
			} else if (remaining < required) {
				// not enough edges left to complete the tree
				break;
			}
		}
		return false;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof CypherPattern)) {
			return false;
		}
		final CypherPattern that = (CypherPattern) obj;
		if (!this.nodeVars.equals(that.nodeVars) || this.relationships.size() != that.relationships.size()) {
			return false;
		}
		return new HashSet<>(this.relationships).equals(new HashSet<>(that.relationships));
	}

	@Override
	public int hashCode() {
		return 31 * this.nodeVars.hashCode() + new HashSet<>(this.relationships).hashCode();
	}
}
