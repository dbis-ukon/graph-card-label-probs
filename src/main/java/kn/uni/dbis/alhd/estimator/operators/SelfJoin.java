package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.estimator.GADbProperties;
import kn.uni.dbis.alhd.estimator.GAResultProperties;
import kn.uni.dbis.alhd.estimator.LabelDistribution;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public final class SelfJoin implements GALogicalOperator {

	private final GADbProperties dbProps;
	private final String stayingVar;
	private final String leavingVar;

	public SelfJoin(
			final GADbProperties dbProps,
			final String retainedVariable,
			final String removedVariable
			) {
		if (retainedVariable.equals(removedVariable)) {
			throw new IllegalArgumentException("The two node variables to be joined must be distinct.");
		}
		this.dbProps = dbProps;
		this.stayingVar = retainedVariable;
		this.leavingVar = removedVariable;
	}

	@Override
	public GAResultProperties computeLogicalProperties(final List<GAResultProperties> inputProperties) {
		final GAResultProperties input = inputProperties.get(0);
		final LabelDistribution labelDist = this.dbProps.getLabelDistribution();
		final List<Set<Integer>> disjoints = new ArrayList<>(labelDist.getLabelHierarchy());
		
		final String tempVar = "$temp";
		final GAResultProperties initial = new GetNodes(this.dbProps, tempVar).computeLogicalProperties(Collections.emptyList());
		final double numAll = this.dbProps.nodes(-1);
		final double[] partSizes = this.disjointSizes(initial, disjoints, tempVar);
		
		final Map<Integer, Double> fractionsAtLeaving = input.getNodeLabelMap().get(this.leavingVar);
		final Map<Integer, Double> fractionsAtStaying = input.getNodeLabelMap().get(this.stayingVar);
		
		// We estimate the expansion degree using the label hierarchy provided by the database properties.
		double estimatedDegree = 0.0d;
		double remainingSt = 1.0d;
		
		final List<Integer> labelOrder = input.sortLabelsByFractionAndRecall(this.stayingVar, this.leavingVar, this.dbProps.labels(), this.dbProps);
		final Map<Integer, Integer> labelToPart = new HashMap<>();
		for (int p = 0; p < disjoints.size(); p++) {
			for (Integer lbl : disjoints.get(p)) {
				labelToPart.put(lbl, p);
			}
		}

		final List<Set<Integer>> uncovered = disjoints.stream().map(HashSet::new).collect(Collectors.toList());
		final double[] coveredFractions = new double[partSizes.length];
		for (int i = 0; i < labelOrder.size() && remainingSt > 0.0d; i++) {
			final int l = labelOrder.get(i);
			final int partID = labelToPart.get(l);
			final Set<Integer> uncoveredLabels = uncovered.get(partID);
			
			// Update the fraction of input nodes explained by the labels in this overlapping set.
			if (coveredFractions[partID] < 1.0d) {
				final double numL = this.dbProps.nodes(l);
				final Set<Integer> subL = labelDist.getAllKnownSublabels(l);
				// Avoid adding degrees multiple times by tracking which labels are already covered by previous ones.
				if (uncoveredLabels.contains(l)) {
					final double fracSt = fractionsAtStaying.getOrDefault(l, 0.0);
					if (fracSt > 0) {
						// add the amount of overlap between nodes with label `l` bound to the remaining variable
						// with nodes in the same partition bound to the leaving variable
						for (final int l2 : uncoveredLabels) {
							final double fracLv = fractionsAtLeaving.getOrDefault(l2, 0.0);
							if (fracLv > 0) {
								final double numL2 = this.dbProps.nodes(l2);
								final Set<Integer> subL2 = labelDist.getAllKnownSublabels(l2);
								final double pL2CondL;
								if (subL.contains(l2)) {
									// `l2` is a sublabel of `l`, so a fixed fraction of nodes qualify
									pL2CondL = numL <= 0 ? 0 : Math.min(1.0, numL2 / numL);
								} else if (l == l2 || subL2.contains(l)) {
									// `l` is a sublabel of `l2`, so all nodes qualify
									pL2CondL = 1;
								} else {
									// assume independence, probability is P[n:L2 | n \in part ] = N(L2) / |part|
									final double pL2 = numL2 / numAll;
									final double pL2InPart = partSizes[partID] <= 0 ? pL2 : pL2 / partSizes[partID];
									pL2CondL = Math.min(pL2InPart, 1);
								}
								final double fracStL2 = pL2CondL * fracSt;
								final double contrib = (1.0d - coveredFractions[partID]) * fracStL2 / numL2;
								// newFracs.put(l2, value)
								estimatedDegree += contrib;
							}
						}
					}
					coveredFractions[partID] += (1.0d - coveredFractions[partID]) * fracSt;
				}
				uncoveredLabels.remove(l);
				uncoveredLabels.removeAll(subL);
			}
			// Logically, the remaining fraction is always at least at big as the covered one. But this might be violated by rounding errors or user errors.
			remainingSt -= Math.min(remainingSt, coveredFractions[partID]);
		}
		
		estimatedDegree += remainingSt * this.dbProps.nodes(-1);
		final double factor = estimatedDegree;

		final Map<String, Map<Integer, Double>> newNLM = new HashMap<>(input.getNodeLabelMap());
		newNLM.remove(this.leavingVar);
		final Map<Integer, Double> newStaying = new HashMap<>(newNLM.get(this.stayingVar));
		for (final Entry<Integer, Double> e : fractionsAtLeaving.entrySet()) {
			newStaying.merge(e.getKey(), e.getValue(), Math::min);
		}
		return new GAResultProperties(newNLM, input.getRelationshipTypeMap(), input.getSize() * factor, false);
	}

	/**
	 * Estimates the relative sizes of the partitions of nodes at a specific node variable.
	 *
	 * @param input input properties
	 * @param parts list of disjoint sets of labels
	 * @param var variable to look at
	 * @return array of relative
	 */
	private double[] disjointSizes(final GAResultProperties input, final List<Set<Integer>> parts, final String var) {
		final double[] sizes = new double[parts.size()];
		if (parts.size() < 2) {
			Arrays.fill(sizes, 1);
			return sizes;
		}

		final Map<Integer, Integer> lblToPart = new HashMap<>();
		for (int i = 0; i < parts.size(); i++) {
			for (final Integer lbl : parts.get(i)) {
				lblToPart.put(lbl, i);
			}
		}
		final Map<Integer, Double> labelDist = input.getNodeLabelMap().get(var);
		double remaining = 1;
		final Set<Integer> alreadyProcessed = new HashSet<>();
		final LabelDistribution labels = this.dbProps.getLabelDistribution();
		final List<Integer> sortedLabels = input.sortLabelsByFractionAndRecall(var, this.dbProps.labels(), this.dbProps);
		for (final Integer next : sortedLabels) {
			if (!alreadyProcessed.contains(next)) {
				alreadyProcessed.addAll(labels.getAllKnownSublabels(next));
				final double part = labelDist.getOrDefault(next, 0.0) * remaining;
				sizes[lblToPart.get(next)] += part;
				remaining -= part;
				if (remaining <= 0) {
					break;
				}
			}
		}

		final double sum = Arrays.stream(sizes).sum();
		if (sum > 0) {
			for (int i = 0; i < sizes.length; i++) {
				sizes[i] /= sum;
			}
		}
		return sizes;
	}

	private double estimateOverlap(final GAResultProperties input) {
		final LabelDistribution labelDist = this.dbProps.getLabelDistribution();
		final List<Set<Integer>> disjoints = new ArrayList<>(labelDist.getLabelHierarchy());

		final String tempVar = "$temp";
		final GAResultProperties initial = new GetNodes(this.dbProps, tempVar).computeLogicalProperties(Collections.emptyList());
		final double numAll = this.dbProps.nodes(-1);
		final double[] partSizes = disjointSizes(initial, disjoints, tempVar);

		final Map<Integer, Double> fractionsAtLeaving = input.getNodeLabelMap().get(this.leavingVar);
		final Map<Integer, Double> fractionsAtStaying = input.getNodeLabelMap().get(this.stayingVar);

		// We estimate the expansion degree using the label hierarchy provided by the database properties.
		double estimatedDegree = 0.0d;
		double remainingSt = 1.0d;

		// Iterate over the sets of overlapping labels.
		for (int i = 0; i < disjoints.size() && remainingSt > 0.0d; i++) {
			// Sort the overlapping labels by fraction and recall in order to get the labels which are the best
			// statistical samples of the nodes at the target variable before the others.
			final Set<Integer> part = disjoints.get(i);
			final Iterator<Integer> overlapping = input.sortLabelsByFractionAndRecall(this.stayingVar, part, this.dbProps).iterator();

			// Update the fraction of input nodes explained by the labels in this overlapping set.
			double coveredFractionSt = 0.0d;
			final Set<Integer> uncoveredLabels = new HashSet<>(part);
			while (overlapping.hasNext() && coveredFractionSt < 1.0d) {
				final int l = overlapping.next();
				final double numL = this.dbProps.nodes(l);
				final Set<Integer> subL = labelDist.getAllKnownSublabels(l);
				// Avoid adding degrees multiple times by tracking which labels are already covered by previous ones.
				if (uncoveredLabels.contains(l)) {
					final double fracSt = fractionsAtStaying.getOrDefault(l, 0.0);
					if (fracSt > 0) {
						// add the amount of overlap between nodes with label `l` bound to the remaining variable
						// with nodes in the same partition bound to the leaving variable
						for (final int l2 : uncoveredLabels) {
							final double fracLv = fractionsAtLeaving.getOrDefault(l2, 0.0);
							if (fracLv > 0) {
								final double numL2 = this.dbProps.nodes(l2);
								final Set<Integer> subL2 = labelDist.getAllKnownSublabels(l2);
								final double pL2CondL;
								if (subL.contains(l2)) {
									// `l2` is a sublabel of `l`, so a fixed fraction of nodes qualify
									pL2CondL = numL <= 0 ? 0 : Math.min(1.0, numL2 / numL);
								} else if (subL2.contains(l)) {
									// `l` is a sublabel of `l2`, so all nodes qualify
									pL2CondL = 1;
								} else {
									// assume independence, probability is P[n:L2 | n \in part ] = N(L2) / |part|
									final double pL2 = numL2 / numAll;
									final double pL2InPart = partSizes[i] <= 0 ? pL2 : pL2 / partSizes[i];
									pL2CondL = Math.min(pL2InPart, 1);
								}
								final double fracStL2 = pL2CondL * fracSt;
								estimatedDegree += (1.0d - coveredFractionSt) * fracStL2 / numL2;
							}
						}
					}
					coveredFractionSt += (1.0d - coveredFractionSt) * fracSt;
				}
				uncoveredLabels.removeAll(subL);
			}
			// Logically, the remaining fraction is always at least at big as the covered one. But this might be violated by rounding errors or user errors.
			remainingSt -= Math.min(remainingSt, coveredFractionSt);
		}
		
		estimatedDegree += remainingSt * this.dbProps.nodes(-1);

		return estimatedDegree;
	}

	@Override
	public int hash() {
		return 31 * stayingVar.hashCode() + leavingVar.hashCode();
	}

	@Override
	public boolean eq(final GALogicalOperator other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof SelfJoin)) {
			return false;
		}
		final SelfJoin that = (SelfJoin) other;
		return this.stayingVar.equals(that.stayingVar) && this.leavingVar.equals(that.leavingVar);
	}

	@Override
	public String toString() {
		return SelfJoin.class.getSimpleName() + "[" + this.stayingVar + " == " + this.leavingVar + "]";
	}
}
