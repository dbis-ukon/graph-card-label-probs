package kn.uni.dbis.alhd.estimator;

import kn.uni.dbis.alhd.statistics.GraphStatistics;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class CSVLabelDistribution extends LabelDistribution {

	public enum Variant {
		ALL,
		ONLY_SUBLABELS,
		ONLY_PARTITION,
		NONE
	}

	public static LabelDistribution read(final GraphStatistics stats, final Path dataPath, final Variant variant) throws IOException {
		Map<Integer, Set<Integer>> sublabelMap = Collections.emptyMap();
		if (variant == Variant.ALL || variant == Variant.ONLY_SUBLABELS) {
			sublabelMap = readSublabels(stats, dataPath);
		}
		final Set<Set<Integer>> labelPartition;
		if (variant == Variant.ALL || variant == Variant.ONLY_PARTITION) {
			final Map<Integer, Set<Integer>> part = readPartition(stats, dataPath);
			labelPartition = new HashSet<>(part.values());
		} else {
			labelPartition = Collections.singleton(
					IntStream.range(0, stats.numLabels()).boxed().collect(Collectors.toUnmodifiableSet()));
		}
		return new CSVLabelDistribution(labelPartition, sublabelMap);
	}

	public static LabelDistribution read(final GraphStatistics stats, final Path dataPath) throws IOException {
		return read(stats, dataPath, Variant.ALL);
	}

	private static Map<Integer, Set<Integer>> readSublabels(final GraphStatistics stats, final Path dataPath)
			throws IOException {
		final Map<Integer,Set<Integer>> sublabelMap = new HashMap<>();
		final Set<String> errors = new HashSet<>();
		try (BufferedReader sublabelIn = Files.newBufferedReader(dataPath.resolve("sublabelMap.csv"))) {
			if (!"label,subLabel".equals(sublabelIn.readLine())) {
				throw new IllegalArgumentException("Wrong header, should be 'label,subLabel'");
			}
            for (String l; (l = sublabelIn.readLine()) != null;) {
            	final String line = l;
            	final int mid = line.indexOf(',');
				final String label = line.substring(0, mid);
				final String subLabel = line.substring(mid + 1);
            	final OptionalInt labelId = stats.getLabelID(label);
				final OptionalInt subLabelId = stats.getLabelID(subLabel);
				if (labelId.isEmpty() || subLabelId.isEmpty()) {
					if (labelId.isEmpty()) {
						errors.add(label);
					}
					if (subLabelId.isEmpty()) {
						errors.add(subLabel);
					}
				} else {
					sublabelMap.computeIfAbsent(labelId.getAsInt(), k -> new HashSet<>()).add(subLabelId.getAsInt());
				}
			}
        }
        if (!errors.isEmpty()) {
        	throw new IllegalArgumentException("Unknown labels: " + errors);
		}
		return sublabelMap;
	}

	private static Map<Integer, Set<Integer>> readPartition(final GraphStatistics stats, final Path dataPath)
			throws IOException {
		final Map<Integer,Set<Integer>> labelPartition = new HashMap<>();
        try (BufferedReader labelPartIn = Files.newBufferedReader(dataPath.resolve("labelPartition.csv"))) {
        	if (!"groupId,label".equals(labelPartIn.readLine())) {
        		throw new IllegalArgumentException("Wrong header, should be 'groupId,label'");
        	}
            for (String line; (line = labelPartIn.readLine()) != null;) {
            	final int mid = line.indexOf(',');
            	final int groupId = Integer.parseInt(line.substring(0, mid));
            	final int labelId = stats.getLabelID(line.substring(mid + 1)).orElseThrow();
            	labelPartition.computeIfAbsent(groupId, k -> new HashSet<>()).add(labelId);
            }
        }
		return labelPartition;
	}

	private CSVLabelDistribution(Set<Set<Integer>> labelHierarchy, Map<Integer, Set<Integer>> sublabelMap) {
		super(labelHierarchy, sublabelMap);
	}
}
