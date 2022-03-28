package kn.uni.dbis.alhd.statistics;

import kn.uni.dbis.alhd.util.IntPair;
import kn.uni.dbis.alhd.util.IntTriple;
import kn.uni.dbis.alhd.util.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

public class  GraphStatistics {

	public enum Key {
		// shared keys between SYN1/SYN2
		OUT,
		IN,
		PATHS,
		PAIRS,

		// additional keys for SYN2
		MIDDLE,
		ONE,
		TWO
	}

	public enum Orientation {
		IN_OUT,
		IN_IN,
		OUT_OUT
	}

	private final long numNodes;
	private final Map<String, Integer> nodeLabelPos;
	private final long[] labelCounts;
	private final Map<String, Integer> edgeTypePos;
	private final Map<Integer, long[]> syn1;
	private final Map<IntTriple, Long> edgeCounts;
	private final Map<IntTriple, long[]> syn2;
	private final Map<String, Integer> propPos;
	private final Map<IntPair, Pair<double[], Map<Integer, Double>>> nodeProps;
	private final Map<IntPair, Pair<double[], Map<Integer, Double>>> relProps;

	public GraphStatistics(final long numNodes,
						   final Map<String, Integer> l2id,
						   final long[] labelCounts,
						   final Map<String, Integer> t2id,
						   final Map<Integer, long[]> syn1,
						   final Map<IntTriple, Long> edgeCounts,
						   final Map<IntTriple, long[]> syn2,
						   final Map<String, Integer> p2id,
						   final Map<IntPair, Pair<double[], Map<Integer, Double>>> nodeProps,
						   final Map<IntPair, Pair<double[], Map<Integer, Double>>> relProps) {
		this.numNodes = numNodes;
		this.nodeLabelPos = l2id;
		this.labelCounts = labelCounts;
		this.edgeTypePos = t2id;
		this.syn1 = syn1;
		this.edgeCounts = edgeCounts;
		this.syn2 = syn2;
		this.propPos = p2id;
		this.nodeProps = nodeProps;
		this.relProps = relProps;
	}

	public static GraphStatistics readFrom(final Path file) throws IOException {
		try (BufferedReader reader = Files.newBufferedReader(file)) {

			startsWith(reader.readLine(), "# Nodes");
			final long numNodes = Long.parseLong(reader.readLine());

			startsWith(reader.readLine(), "# Node Labels");
			final int numLabels = Integer.parseInt(reader.readLine());
			final long[] labelCounts = new long[numLabels];
			final Map<String, Integer> l2id = new HashMap<>();
			for (int i = 0; i < numLabels; i++) {
				final String[] line = reader.readLine().split("\t");
				final int pos = Integer.parseInt(line[1]);
				l2id.put(line[0], pos);
				labelCounts[pos] = Long.parseLong(line[2]);
			}

			startsWith(reader.readLine(), "# Edge Types");
			final int e = Integer.parseInt(reader.readLine());
			final Map<Integer, long[]> syn1 = new HashMap<>(e);
			final Map<String, Integer> t2id = new HashMap<>();
			for (int i = 0; i < e; i++) {
				final String[] line = reader.readLine().split("\t");
				final int pos = Integer.parseInt(line[1]);
				t2id.put(line[0], pos);
				final long[] row = new long[4];
				row[Key.OUT.ordinal()] = Long.parseLong(line[2]);
				row[Key.IN.ordinal()] = Long.parseLong(line[3]);
				row[Key.PATHS.ordinal()] = Long.parseLong(line[4]);
				row[Key.PAIRS.ordinal()] = Long.parseLong(line[5]);
				syn1.put(pos, row);
			}

			startsWith(reader.readLine(), "# Node Properties");
			final int p = Integer.parseInt(reader.readLine());
			final Map<String, Integer> p2id = new HashMap<>();
			for (int i = 0; i < p; i++) {
				final String[] line = reader.readLine().split("\t");
				final int pos = Integer.parseInt(line[1]);
				p2id.put(line[0], pos);
			}

			startsWith(reader.readLine(), "# Label/Type Combinations");
			final int lt = Integer.parseInt(reader.readLine());
			final Map<IntTriple, Long> edgeCounts = new HashMap<>();
			for (int i = 0; i < lt; i++) {
				final String[] line = reader.readLine().split("\t");
				final int v = Integer.parseInt(line[0]);
				final int t = Integer.parseInt(line[1]);
				final int w = Integer.parseInt(line[2]);
				edgeCounts.put(new IntTriple(v, t, w), Long.parseLong(line[3]));
			}

			startsWith(reader.readLine(), "# Type/Type Combinations");
			final int tt = Integer.parseInt(reader.readLine());
			final Map<IntTriple, long[]> syn2 = new HashMap<>();
			for (int i = 0; i < tt; i++) {
				final String[] line = reader.readLine().split("\t");
				final int v = Integer.parseInt(line[0]);
				final int t = Integer.parseInt(line[1]);
				final int w = Integer.parseInt(line[2]);
				final long[] s2 = new long[7];
				s2[Key.OUT.ordinal()] = Long.parseLong(line[3]);
				s2[Key.IN.ordinal()] = Long.parseLong(line[4]);
				s2[Key.MIDDLE.ordinal()] = Long.parseLong(line[5]);
				s2[Key.PATHS.ordinal()] = Long.parseLong(line[6]);
				s2[Key.PAIRS.ordinal()] = Long.parseLong(line[7]);
				s2[Key.ONE.ordinal()] = Long.parseLong(line[8]);
				s2[Key.TWO.ordinal()] = Long.parseLong(line[9]);
				syn2.put(new IntTriple(v, t, w), s2);
			}

			startsWith(reader.readLine(), "# Label/Property Combinations " +
					"(label, property, count, unique, numeric, num_mf, most_frequent..., histogram...)");
			final int lp = Integer.parseInt(reader.readLine());
			final Map<IntPair, Pair<double[], Map<Integer, Double>>> nodeProps = new HashMap<>();
			for (int i = 0; i < lp; i++) {
				final String[] line = reader.readLine().split("\t");
				final int label = Integer.parseInt(line[0]);
				final int prop = Integer.parseInt(line[1]);
				final Pair<double[], Map<Integer, Double>> res = readPropStats(line);
				nodeProps.put(new IntPair(label, prop), res);
			}

			startsWith(reader.readLine(), "# Type/Property Combinations " +
					"(type, property, count, unique, numeric, num_mf, most_frequent..., histogram...)");
			final int tp = Integer.parseInt(reader.readLine());
			final Map<IntPair, Pair<double[], Map<Integer, Double>>> relProps = new HashMap<>();
			for (int i = 0; i < tp; i++) {
				final String[] line = reader.readLine().split("\t");
				final int type = Integer.parseInt(line[0]);
				final int prop = Integer.parseInt(line[1]);
				final Pair<double[], Map<Integer, Double>> res = readPropStats(line);
				relProps.put(new IntPair(type, prop), res);
			}

			return new GraphStatistics(numNodes, l2id, labelCounts, t2id, syn1, edgeCounts, syn2, p2id, nodeProps, relProps);
		}
	}

	private static Pair<double[], Map<Integer, Double>> readPropStats(String[] line) {
		final int count = Integer.parseInt(line[2]);
		final int unique = Integer.parseInt(line[3]);
		final int numeric = Integer.parseInt(line[4]);
		final int numMF = Integer.parseInt(line[5]);
		final Map<Integer, Double> mostFrequent = new LinkedHashMap<>();
		for (int j = 0; j < numMF; j++) {
			final String[] kv = line[j + 6].split("=", 2);
			mostFrequent.put((int) Long.parseLong(kv[0], 16), Double.parseDouble(kv[1]));
		}
		final double[] histo = Arrays.stream(line).skip(numMF + 6).mapToDouble(Double::parseDouble).toArray();
		final double[] counts = new double[histo.length + 3];
		counts[0] = count;
		counts[1] = unique;
		counts[2] = numeric;
		System.arraycopy(histo, 0, counts, 3, histo.length);
		return Pair.of(counts, mostFrequent);
	}

	private static void startsWith(final String line, final String start) {
		if (line == null || !line.startsWith(start)) {
			throw new AssertionError(line == null ? "null" : line);
		}
	}

	public String[] labelNames() {
		return this.nodeLabelPos.keySet().toArray(new String[0]);
	}

	public String[] typeNames() {
		return this.edgeTypePos.keySet().toArray(new String[0]);
	}

	public OptionalInt getLabelID(final String labelName) {
		return this.nodeLabelPos.containsKey(labelName)
				? OptionalInt.of(this.nodeLabelPos.get(labelName)) : OptionalInt.empty();
	}

	public OptionalInt getTypeID(final String typeName) {
		return this.edgeTypePos.containsKey(typeName)
				? OptionalInt.of(this.edgeTypePos.get(typeName)) : OptionalInt.empty();
	}

	public double out(final String edgeLabel) {
		return this.syn1.get(this.edgeTypePos.get(edgeLabel))[Key.OUT.ordinal()];
	}

	public double in(final String edgeLabel) {
		return this.syn1.get(this.edgeTypePos.get(edgeLabel))[Key.IN.ordinal()];
	}

	public double numPaths(final String edgeLabel) {
		return this.syn1.get(this.edgeTypePos.get(edgeLabel))[Key.PATHS.ordinal()];
	}

	public double numPairs(final String edgeLabel) {
		return this.syn1.get(this.edgeTypePos.get(edgeLabel))[Key.PAIRS.ordinal()];
	}

	public double middle(final String l1, final String l2, final Orientation orientation) {
		final IntTriple key = new IntTriple(orientation.ordinal(),
				this.edgeTypePos.get(l1), this.edgeTypePos.get(l2));
		final long[] res = this.syn2.get(key);
		return res == null ? 0 : res[Key.MIDDLE.ordinal()];
	}

	public double syn1(final Key key, final String type, final boolean rev) {
		final long[] syn = this.syn1.get(this.edgeTypePos.get(type));
		switch (key) {
		case IN:
			return syn[(rev ? Key.OUT : Key.IN).ordinal()];
		case OUT:
			return syn[(rev ? Key.IN : Key.OUT).ordinal()];
		case PAIRS:
		case PATHS:
			return syn[key.ordinal()];
		default:
			throw new IllegalArgumentException("Not available for single edge: " + key);
		}
	}

	public double syn2(final Key key, final String l1, final boolean rev1, final String l2, final boolean rev2) {
		final int t1 = this.edgeTypePos.get(l1);
		final int t2 = this.edgeTypePos.get(l2);
		final int a;
		final int b;
		final Orientation o;
		if (!rev1) {
			if (!rev2) {
				a = t1;
				b = t2;
				o = Orientation.IN_OUT;
			} else {
				a = Math.min(t1, t2);
				b = Math.max(t1, t2);
				o = Orientation.IN_IN;
			}
		} else {
			if (!rev2) {
				a = Math.min(t1, t2);
				b = Math.max(t1, t2);
				o = Orientation.OUT_OUT;
			} else {
				a = t2;
				b = t1;
				o = Orientation.IN_OUT;
			}
		}
		final long[] res = this.syn2.get(new IntTriple(o.ordinal(), a, b));
		return res == null ? 0 : res[key.ordinal()];
	}

	public double numPairs(final String l1, final String l2, final Orientation orientation) {
		final IntTriple key = new IntTriple(orientation.ordinal(),
				this.edgeTypePos.get(l1), this.edgeTypePos.get(l2));
		final long[] res = this.syn2.get(key);
		return res == null ? 0 : res[Key.PAIRS.ordinal()];
	}

	public double numOne(String l1, String l2, Orientation orientation) {
		final IntTriple key = new IntTriple(orientation.ordinal(),
				this.edgeTypePos.get(l1), this.edgeTypePos.get(l2));
		final long[] res = this.syn2.get(key);
		return res == null ? 0 : res[Key.ONE.ordinal()];
	}

	public double numTwo(String l1, String l2, Orientation orientation) {
		final IntTriple key = new IntTriple(orientation.ordinal(),
				this.edgeTypePos.get(l1), this.edgeTypePos.get(l2));
		final long[] res = this.syn2.get(key);
		return res == null ? 0 : res[Key.TWO.ordinal()];
	}

	public double out(String l1, String l2, Orientation orientation) {
		final IntTriple key = new IntTriple(orientation.ordinal(),
				this.edgeTypePos.get(l1), this.edgeTypePos.get(l2));
		final long[] res = this.syn2.get(key);
		return res == null ? 0 : res[Key.OUT.ordinal()];
	}

	public double in(String l1, String l2, Orientation orientation) {
		final Key synKey = Key.IN;
		final IntTriple key = new IntTriple(orientation.ordinal(),
				this.edgeTypePos.get(l1), this.edgeTypePos.get(l2));
		final long[] res = this.syn2.get(key);
		return res == null ? 0 : res[synKey.ordinal()];
	}

	public double relCount(int labelAtBase, int type, int labelAtTarget) {
		return this.edgeCounts.getOrDefault(new IntTriple(labelAtBase, type, labelAtTarget), 0L);
	}

	public double relCount(String labelAtBase, String type, String labelAtTarget) {
		if (labelAtBase != null && !this.nodeLabelPos.containsKey(labelAtBase)
				|| type != null && !this.edgeTypePos.containsKey(type)
				|| labelAtTarget != null && !this.nodeLabelPos.containsKey(labelAtTarget)) {
			return 0;
		}
		final int v = labelAtBase == null ? -1 : this.nodeLabelPos.get(labelAtBase);
		final int t = type == null ? -1 : this.edgeTypePos.get(type);
		final int w = labelAtTarget == null ? -1 : this.nodeLabelPos.get(labelAtTarget);
		return this.edgeCounts.getOrDefault(new IntTriple(v, t, w), 0L);
	}

	public double numNodes(int label) {
		return label == -1 ? this.numNodes : this.labelCounts[label];
	}

	public double numNodes(final String label) {
		final Integer v = label == null ? Integer.valueOf(-1) : this.nodeLabelPos.get(label);
		return v == null ? 0 : this.labelCounts[v];
	}

	public double numRelationships(int type) {
		return this.relCount(-1, type, -1);
	}

	public double numRelationships(final String type) {
		final Integer v = type == null ? Integer.valueOf(-1) : this.edgeTypePos.get(type);
		return v == null ? 0 : this.labelCounts[v];
	}

	public final Map<String, Integer> getPropertyIDs() {
		return Collections.unmodifiableMap(this.propPos);
	}

	public final Integer getPropertyID(final String propName) {
		return this.propPos.get(propName);
	}

	public double nodeWithProperty(int label, int property) {
		final Pair<double[], Map<Integer, Double>> rec = this.nodeProps.get(new IntPair(label, property));
		return rec == null ? 0 : rec.getFirst()[0];
	}

	public double nodeWithPropertyUnique(int label, int property) {
		final Pair<double[], Map<Integer, Double>> rec = this.nodeProps.get(new IntPair(label, property));
		return rec == null ? 0 : rec.getFirst()[1];
	}

	public double nodeWithPropertyNumeric(int label, int property) {
		final Pair<double[], Map<Integer, Double>> rec = this.nodeProps.get(new IntPair(label, property));
		return rec == null ? 0 : rec.getFirst()[2];
	}

	public double relWithProperty(int type, int property) {
		final Pair<double[], Map<Integer, Double>> rec = this.relProps.get(new IntPair(type, property));
		return rec == null ? 0 : rec.getFirst()[0];
	}

	public double relWithPropertyUnique(int type, int property) {
		final Pair<double[], Map<Integer, Double>> rec = this.relProps.get(new IntPair(type, property));
		return rec == null ? 0 : rec.getFirst()[1];
	}

	public double relWithPropertyNumeric(int type, int property) {
		final Pair<double[], Map<Integer, Double>> rec = this.relProps.get(new IntPair(type, property));
		return rec == null ? 0 : rec.getFirst()[2];
	}

	public OptionalDouble nodePropertyRange(final int label, final int property, final double min, final double max) {
		if (min > max) {
			throw new IllegalArgumentException(String.format(Locale.US, "Broken range: [%s, %s]", min, max));
		}
		final Pair<double[], Map<Integer, Double>> rec = this.nodeProps.get(new IntPair(label, property));
		if (rec == null || rec.getFirst()[2] == 0) {
			return OptionalDouble.empty();
		}
		return OptionalDouble.of(range(rec.getFirst(), min, max));
	}

	public OptionalDouble nodePropIfFrequent(final int label, final int property, final int hash) {
		final Pair<double[], Map<Integer, Double>> rec = this.nodeProps.get(new IntPair(label, property));
		if (rec == null) {
			return OptionalDouble.empty();
		}
		final Double part = rec.getSecond().get(hash);
		return part == null ? OptionalDouble.empty() : OptionalDouble.of(part);
	}

	public int numNodePropFrequent(final int label, final int property) {
		final Pair<double[], Map<Integer, Double>> rec = this.nodeProps.get(new IntPair(label, property));
		return rec == null ? 0 : rec.getSecond().size();
	}

	public double nodePropNonFrequent(final int label, final int property, final int hash) {
		final Pair<double[], Map<Integer, Double>> rec = this.nodeProps.get(new IntPair(label, property));
		if (rec == null) {
			return 1.0;
		}
		return Math.max(0, Math.min(1 - rec.getSecond().values().stream().mapToDouble(Double::doubleValue).sum(), 1));
	}

	public OptionalDouble relPropIfFrequent(final int label, final int property, final int hash) {
		final Pair<double[], Map<Integer, Double>> rec = this.relProps.get(new IntPair(label, property));
		if (rec == null) {
			return OptionalDouble.empty();
		}
		final Double part = rec.getSecond().get(hash);
		return part == null ? OptionalDouble.empty() : OptionalDouble.of(part);
	}

	public int numRelPropFrequent(final int label, final int property) {
		final Pair<double[], Map<Integer, Double>> rec = this.relProps.get(new IntPair(label, property));
		return rec == null ? 0 : rec.getSecond().size();
	}

	public double relPropNonFrequent(final int label, final int property, final int hash) {
		final Pair<double[], Map<Integer, Double>> rec = this.relProps.get(new IntPair(label, property));
		if (rec == null) {
			return 1.0;
		}
		return Math.max(0, Math.min(1 - rec.getSecond().values().stream().mapToDouble(Double::doubleValue).sum(), 1));
	}

	public OptionalDouble relPropertyRange(final int type, final int property, final double min, final double max) {
		if (min > max) {
			throw new IllegalArgumentException(String.format(Locale.US, "Broken range: [%s, %s]", min, max));
		}
		final Pair<double[], Map<Integer, Double>> rec = this.relProps.get(new IntPair(type, property));
		if (rec == null || rec.getFirst()[2] == 0) {
			return OptionalDouble.empty();
		}
		return OptionalDouble.of(range(rec.getFirst(), min, max));
	}

	private static double range(final double[] quantiles, final double vmin, final double vmax) {
		final int start = 3;
		final double min = Math.max(vmin, quantiles[start]);
		final double max = Math.min(vmax, quantiles[quantiles.length - 1]);
		final int buckets = quantiles.length - start - 1;
		int l = start;
		int r = quantiles.length - 1;
		while (l + 1 < quantiles.length && quantiles[l + 1] < min) {
			l++;
		}
		if (l + 1 < quantiles.length && quantiles[l + 1] == min) {
			l++;
		}
		while (r - 1 >= start && quantiles[r - 1] > max) {
			r--;
		}
		if (r - 1 >= start && quantiles[r - 1] == max) {
			r--;
		}
		if (r <= l) {
			return 0.0;
		}
		if (l + 1 == r) {
			final double bucketRange = quantiles[r] - quantiles[l];
			final double fracOfBucket = (max - min) / bucketRange;
			return fracOfBucket / buckets;
		}
		final int involved = r - l;
		final double firstFrac = min < quantiles[l] || quantiles[l + 1] == quantiles[l] ? 1 :
				(quantiles[l + 1] - min) / (quantiles[l + 1] - quantiles[l]);
		final double lastFrac = max > quantiles[r] || quantiles[r] == quantiles[r - 1] ? 1 :
				(max - quantiles[r - 1]) / (quantiles[r] - quantiles[r - 1]);

		return (firstFrac + involved - 2.0 + lastFrac) / buckets;
	}

	public int numLabels() {
		return this.nodeLabelPos.size();
	}

	public int numTypes() {
		return this.edgeTypePos.size();
	}

	public Optional<String> getLabelName(final int label) {
		return this.nodeLabelPos.entrySet().stream().filter(e -> e.getValue().equals(label))
				.map(Map.Entry::getKey).findFirst();
	}

	public Optional<String> getTypeName(final int type) {
		return this.edgeTypePos.entrySet().stream().filter(e -> e.getValue().equals(type))
				.map(Map.Entry::getKey).findFirst();
	}

	public Map<String, Integer> sizeStats() {
		final Map<String, Integer> counts = new LinkedHashMap<>();
		counts.put("reltype_counts_n4j", Math.toIntExact(edgeCounts.entrySet().stream()
				.filter(e -> e.getKey().getFirst() == -1 || e.getKey().getThird() == -1)
				.count()));
		counts.put("reltype_counts_all", edgeCounts.size());
		counts.put("nodelabel_counts", this.labelCounts.length + 1);
		counts.put("num_nodelabel", nodeLabelPos.size());
		counts.put("nodelabel_strings", Math.toIntExact(nodeLabelPos.keySet().stream().mapToInt(String::length).sum()));
		counts.put("num_reltypes", edgeTypePos.size());
		counts.put("reltype_strings", Math.toIntExact(edgeTypePos.keySet().stream().mapToInt(String::length).sum()));
		counts.put("num_props", propPos.size());
		counts.put("prop_strings", Math.toIntExact(propPos.keySet().stream().mapToInt(String::length).sum()));
		counts.put("prop_entries_num", Math.toIntExact(Stream.of(this.nodeProps, this.relProps)
				.flatMap(m -> m.values().stream()).filter(p -> p.getFirst()[2] != 0).count()));
		counts.put("prop_entries_str", Math.toIntExact(Stream.of(this.nodeProps, this.relProps)
				.flatMap(m -> m.values().stream()).filter(p -> p.getFirst()[2] == 0).count()));
		return counts;
	}
}
