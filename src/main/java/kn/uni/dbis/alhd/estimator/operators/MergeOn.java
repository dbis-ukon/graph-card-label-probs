package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.estimator.GADbProperties;
import kn.uni.dbis.alhd.estimator.GAResultProperties;
import kn.uni.dbis.alhd.estimator.LabelDistribution;

import java.util.*;
import java.util.Map.Entry;

public final class MergeOn implements GALogicalOperator {

	private final GADbProperties dbProps;
	private final String stayingVar;
	private final String leavingVar;

	public MergeOn(
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
		final Map<Integer, Integer> labelToCluster = new HashMap<>();
		for (int i = 0; i < disjoints.size(); i++) {
			for (final Integer label : disjoints.get(i)) {
				labelToCluster.put(label, i);
			}
		}

		final Map<Integer, Double> fractionsAtLeaving = input.getNodeLabelMap().get(this.leavingVar);
		final Map<Integer, Double> fractionsAtStaying = input.getNodeLabelMap().get(this.stayingVar);
		
		// We estimate the expansion degree using the label hierarchy provided by the database properties.
		double estimatedSelectivity = 0.0d;
		double remainingSt = 1.0d;
		double remainingLv = 1.0d;

		final Map<Integer, Double> oldPartFrac = new HashMap<>();
		final Map<Integer, Double> newPartFrac = new HashMap<>();
        // Iterate over the sets of overlapping labels.
        for (int i = 0; i < disjoints.size() && Math.min(remainingSt, remainingLv) > 0.0d; i++) {
            // Sort the overlapping labels by fraction and recall in order to get the labels which are the best
            // statistical samples of the nodes at the base variable before the others.
        	final Set<Integer> dI = disjoints.get(i);
    		final Iterator<Integer> overlapping =
    				input.sortLabelsByFractionAndRecall(this.stayingVar, this.leavingVar, dI, this.dbProps).iterator();

            // The fraction of nodes not explained by labels from other partition member sets.
            final double oldRemainingSt = remainingSt;
            final double oldRemainingLv = remainingLv;

            // Keep a minimal set of superlabels, no sublabels of the member labels are allowed in this set.
            final Set<Integer> superLabels = new HashSet<>();

            // The covered sublabels of already processed labels.
            final Set<Integer> coveredLabels = new HashSet<>();

            double inContr = 0;
            double outContr = 0;

            while (overlapping.hasNext() && Math.min(remainingSt, remainingLv) > 0.0d) {
                final int l = overlapping.next();
                // The fraction of nodes not covered by the current set of superlabels.
                double notCoveredBySuperLabelsSt = 1.0d;
                double notCoveredBySuperLabelsLv = 1.0d;

                // Do not add the degree of nodes having already covered labels.
                if (!coveredLabels.contains(l)) {
                    // Remove all previous labels covered by the current label.
                    final boolean superLabelsChanged = superLabels.removeAll(labelDist.getAllKnownSublabels(l));
                    superLabels.add(l);
                    if (superLabelsChanged) {
                        notCoveredBySuperLabelsSt = superLabels.stream()
                                .mapToDouble(s -> 1.0d - fractionsAtStaying.get(s))
                                .reduce((f1, f2) -> f1 * f2).getAsDouble();
                        notCoveredBySuperLabelsLv = superLabels.stream()
                                .mapToDouble(s -> 1.0d - fractionsAtLeaving.get(s))
                                .reduce((f1, f2) -> f1 * f2).getAsDouble();
                    } else {
                        // Avoid re-computation of the product if the superlabels have not changed.
                        notCoveredBySuperLabelsSt *= 1.0d - fractionsAtStaying.get(l);
                        notCoveredBySuperLabelsLv *= 1.0d - fractionsAtLeaving.get(l);
                    }

                    // Compute the new fraction of nodes that have not yet been represented by a label.
                    // Logically, the fraction covered by labels from other partition member sets cannot be bigger than
                    // the fraction not covered by the labels in the current partition member set.
                    // However, this can be violated by rounding errors or user errors.
                    final double newRemainingSt = Math.min(remainingSt, Math.max(0.0d, notCoveredBySuperLabelsSt - (1.0d - oldRemainingSt)));
                    final double newRemainingLv = Math.min(remainingLv, Math.max(0.0d, notCoveredBySuperLabelsLv - (1.0d - oldRemainingLv)));

                    // Compute the fraction of nodes represented by the current label.
                    final double fractionOfLSt = remainingSt - newRemainingSt;
                    final double fractionOfLLv = remainingLv - newRemainingLv;

                    inContr += Math.min(fractionOfLSt, fractionOfLLv);
                    final double contribution = fractionOfLSt * fractionOfLLv / this.dbProps.nodes(l);
                    outContr += contribution;
                    estimatedSelectivity += contribution;
                    remainingSt = newRemainingSt;
                    remainingLv = newRemainingLv;
                    
                }
                coveredLabels.addAll(labelDist.getAllKnownSublabels(l));
            }
    		oldPartFrac.put(i, inContr);
    		newPartFrac.put(i, outContr);
        }

        // Use the average degree in the database for nodes having no label.
        estimatedSelectivity += remainingSt * remainingLv / this.dbProps.nodes(-1);

		final double factor = estimatedSelectivity;

//		System.out.println(oldPartFrac + " vs. " + newPartFrac);
		final Map<String, Map<Integer, Double>> newNLM = new HashMap<>(input.getNodeLabelMap());
		newNLM.remove(this.leavingVar);
		final Map<Integer, Double> newStaying = new HashMap<>(newNLM.get(this.stayingVar));
		for (final Entry<Integer, Double> e : fractionsAtLeaving.entrySet()) {
			final Integer label = e.getKey();
			final Integer cluster = labelToCluster.get(label);
			final Double fracLv = e.getValue();
			final Double fracSt = newStaying.get(label);
			final double minFrac = Math.min(fracSt, fracLv);
			final double inClFrac = oldPartFrac.getOrDefault(cluster, 0.0);
			final double outClFrac = newPartFrac.getOrDefault(cluster, 0.0);
			final double res = minFrac == 0 || factor == 0 ? 0 : minFrac / factor;
			newStaying.put(label, Math.max(0, Math.min(res, 1)));
//			System.out.println(minFrac + " vs. " + (minFrac == 0 ? 0 : outClFrac / inClFrac));
			//System.out.println(factor);
		}
		newNLM.put(this.stayingVar, newStaying);
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
		if (!(other instanceof MergeOn)) {
			return false;
		}
		final MergeOn that = (MergeOn) other;
		return this.stayingVar.equals(that.stayingVar) && this.leavingVar.equals(that.leavingVar);
	}

	@Override
	public String toString() {
		return MergeOn.class.getSimpleName() + "[" + this.stayingVar + " == " + this.leavingVar + "]";
	}
}
