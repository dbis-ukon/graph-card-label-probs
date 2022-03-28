package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.queries.Direction;
import kn.uni.dbis.alhd.estimator.GADbProperties;
import kn.uni.dbis.alhd.estimator.GAResultProperties;
import kn.uni.dbis.alhd.estimator.LabelDistribution;

import java.util.*;

/**
 * Implementation of the Expand-Operator.
 */
public class Expand implements GALogicalOperator {
    /** Database properties. */
    private final GADbProperties dbProps;

    /** Variable that matches the nodes to be expanded. */
    private final String baseVariable;

    /** Variable that matches the relationships found by this expansion. */
    private final String relationshipVariable;

    /** Types of the relationship variable. */
    private final Set<Integer> relationshipTypes;

    /** Variable that matches the new nodes found by this expansion. */
    private final String targetVariable;

    /** Direction of the expansion. */
    private final Direction direction;

    /**
     * Constructs a new Expand-operator.
     *
     * @param dbProps database properties
     * @param baseVariable which nodes should be expanded from the input
     * @param direction whether the expansion uses outgoing or incoming edges
     * @param relationshipVariable how to match the relationships found by this expansion
     * @param relationshipTypes the types of relationships found by this expansion
     *                          (the empty set represents all types)
     * @param targetVariable how to match the new nodes found by this expansion
     */
    public Expand(
            final GADbProperties dbProps,
            final String baseVariable,
            final Direction direction,
            final String relationshipVariable,
            final Set<Integer> relationshipTypes,
            final String targetVariable) {
        this.dbProps = dbProps;
        this.baseVariable = baseVariable;
        this.direction = direction;
        this.relationshipVariable = relationshipVariable;
        this.relationshipTypes = relationshipTypes;
        this.targetVariable = targetVariable;
    }

    /**
     * Helper constructor for outgoing expansions.
     *
     * @param dbProps database properties
     * @param baseVariable which nodes should be expanded from the input
     * @param relationshipVariable how to match the relationships found by this expansion
     * @param relationshipTypes the types of relationships found by this expansion
     *                          (the empty set represents all types)
     * @param newVariable how to match the new nodes found by this expansion
     * @return the new expand operator
     */
    public static Expand both(
            final GADbProperties dbProps,
            final String baseVariable,
            final String relationshipVariable,
            final Set<Integer> relationshipTypes,
            final String newVariable) {
        return new Expand(dbProps, baseVariable, Direction.BOTH, relationshipVariable, relationshipTypes, newVariable);
    }

    /**
     * Helper constructor for outgoing expansions.
     *
     * @param dbProps database properties
     * @param baseVariable which nodes should be expanded from the input
     * @param relationshipVariable how to match the relationships found by this expansion
     * @param relationshipTypes the types of relationships found by this expansion
     *                          (the empty set represents all types)
     * @param newVariable how to match the new nodes found by this expansion
     * @return the new expand operator
     */
    public static Expand out(
            final GADbProperties dbProps,
            final String baseVariable,
            final String relationshipVariable,
            final Set<Integer> relationshipTypes,
            final String newVariable) {
        return new Expand(dbProps, baseVariable, Direction.OUTGOING, relationshipVariable, relationshipTypes, newVariable);
    }

    /**
     * Helper constructor for incoming expansions.
     *
     * @param dbProps database properties
     * @param baseVariable which nodes should be expanded from the input
     * @param relationshipVariable how to match the relationships found by this expansion
     * @param relationshipTypes the types of relationships found by this expansion
     *                          (the empty set represents all types)
     * @param newVariable how to match the new nodes found by this expansion
     * @return the new expand operator
     */
    public static Expand in(
            final GADbProperties dbProps,
            final String baseVariable,
            final String relationshipVariable,
            final Set<Integer> relationshipTypes,
            final String newVariable) {
        return new Expand(dbProps, baseVariable, Direction.INCOMING, relationshipVariable, relationshipTypes, newVariable);
    }

    /**
     * Computes an estimate for the amount of relationships per input node found by this expand which
     * end at a node with the given label.
     * The estimate is based on the known degrees of nodes with certain labels.
     *
     * @param input the logical properties of the input of this expand
     * @param labelAtTarget which label the target node of the relationship must have
     * @return how many relationships are expected to be found by this expand to the given label
     */
    private double estimateDegree(final GAResultProperties input, final int labelAtTarget) {
        final Map<Integer, Double> fractionsAtBase = input.getNodeLabelMap().get(this.baseVariable);
        final LabelDistribution labelDist = this.dbProps.getLabelDistribution();
        // We estimate the expansion degree using the label hierarchy provided by the database properties.
        final Iterator<Set<Integer>> disjoint = labelDist.getLabelHierarchy().iterator();

        // The degree of the expand according to the already processed labels.
        double estimatedDegree = 0.0d;

        // The fraction of nodes which have not yet been represented by a label.
        double remaining = 1.0d;

        // Iterate over the sets of overlapping labels.
        while (disjoint.hasNext() && remaining > 0.0d) {
            // Sort the overlapping labels by fraction and recall in order to get the labels which are the best
            // statistical samples of the nodes at the base variable before the others.
            final Iterator<Integer> overlapping = input.sortLabelsByFractionAndRecall(this.baseVariable, disjoint.next(), this.dbProps).iterator();

            // The fraction of nodes not explained by labels from other partition member sets.
            final double oldRemaining = remaining;

            // Keep a minimal set of superlabels, no sublabels of the member labels are allowed in this set.
            final Set<Integer> superLabels = new HashSet<>();

            // The covered sublabels of already processed labels.
            final Set<Integer> coveredLabels = new HashSet<>();

            while (overlapping.hasNext() && remaining > 0.0d) {
                final int l = overlapping.next();
                // The fraction of nodes not covered by the current set of superlabels.
                double notCoveredBySuperLabels = 1.0d;

                // Do not add the degree of nodes having already covered labels.
                if (!coveredLabels.contains(l)) {
                    // Remove all previous labels covered by the current label.
                    final boolean superLabelsChanged = superLabels.removeAll(labelDist.getAllKnownSublabels(l));
                    superLabels.add(l);
                    if (superLabelsChanged) {
                        notCoveredBySuperLabels = superLabels.stream()
                                .mapToDouble(s -> 1.0d - fractionsAtBase.get(s))
                                .reduce((f1, f2) -> f1 * f2).orElseThrow();
                    } else {
                        // Avoid re-computation of the product if the superlabels have not changed.
                        notCoveredBySuperLabels *= 1.0d - fractionsAtBase.get(l);
                    }

                    // Compute the new fraction of nodes that have not yet been represented by a label.
                    // Logically, the fraction covered by labels from other partition member sets cannot be bigger than
                    // the fraction not covered by the labels in the current partition member set.
                    // However, this can be violated by rounding errors or user errors.
                    final double newRemaining = Math.min(remaining, Math.max(0.0d, notCoveredBySuperLabels - (1.0d - oldRemaining)));

                    // Compute the fraction of nodes represented by the current label.
                    final double fractionOfL = remaining - newRemaining;

                    estimatedDegree += dbProps.averageDegree(l, this.relationshipTypes, labelAtTarget, this.direction) * fractionOfL;
                    remaining = newRemaining;
                }
                coveredLabels.addAll(labelDist.getAllKnownSublabels(l));
            }
        }
        // Use the average degree in the database for nodes having no label.
        estimatedDegree += dbProps.averageDegree(-1, this.relationshipTypes, labelAtTarget, direction) * remaining;

        return estimatedDegree;
    }

    /**
     * Computes an estimate for the amount of relationships per input node found by this expand.
     *
     * @param input the logical properties of the input of this expand
     * @return how many relationships are expected to be found by this expand
     */
    private double estimateDegree(final GAResultProperties input) {
        // do we estimate the number of relationships to a fresh variable?
        if (!input.anyNodeMatchedBy(this.targetVariable)) {
            return this.estimateDegree(input, -1);
        }

        final Map<Integer, Double> fractionsAtTarget = input.getNodeLabelMap().get(this.targetVariable);
        final LabelDistribution labelDist = this.dbProps.getLabelDistribution();
        final Iterator<Set<Integer>> disjoint = labelDist.getLabelHierarchy().iterator();

        // We estimate the expansion degree using the label hierarchy provided by the database properties.
        double estimatedDegree = 0.0d;
        double remaining = 1.0d;

        // Iterate over the sets of overlapping labels.
        while (disjoint.hasNext() && remaining > 0.0d) {
            // Sort the overlapping labels by fraction and recall in order to get the labels which are the best
            // statistical samples of the nodes at the target variable before the others.
            final Iterator<Integer> overlapping = input.sortLabelsByFractionAndRecall(this.targetVariable, disjoint.next(), this.dbProps).iterator();

            // Update the fraction of input nodes explained by the labels in this overlapping set.
            double coveredFraction = 0.0d;
            final Set<Integer> coveredLabels = new HashSet<>();

            while (overlapping.hasNext() && coveredFraction < 1.0d) {
                final int l = overlapping.next();
                // Avoid adding degrees multiple times by tracking which labels are already covered by previous ones.
                if (!coveredLabels.contains(l)) {
                    estimatedDegree += (1.0d - coveredFraction) * this.estimateDegree(input, l) * fractionsAtTarget.get(l) / this.dbProps.nodes(l);
                    coveredFraction += (1.0d - coveredFraction) * fractionsAtTarget.get(l);
                }
                coveredLabels.addAll(labelDist.getAllKnownSublabels(l));
            }
            // Logically, the remaining fraction is always at least at big as the covered one. But this might be violated by rounding errors or user errors.
            remaining -= Math.min(remaining, coveredFraction);
        }
        estimatedDegree += remaining * this.estimateDegree(input, -1) / this.dbProps.nodes(-1);

        return estimatedDegree;
    }

    @Override
    public GAResultProperties computeLogicalProperties(final List<GAResultProperties> inputProperties) {
        final GAResultProperties input = inputProperties.get(0);
        final Map<String, Set<Integer>> relationshipTypeMap = new HashMap<>(input.getRelationshipTypeMap());

        if (!input.anyNodeMatchedBy(this.baseVariable)) {
            throw new IllegalArgumentException("Base variable of expansion must be matched in the input.");
        } else if (input.anyRelationshipMatchedBy(this.relationshipVariable)) {
            throw new IllegalArgumentException("Relationship variable of expansion must not be matched in the input.");
        }
        
        // Set the allowed relationship types.
        relationshipTypeMap.put(this.relationshipVariable, this.relationshipTypes);

        final Map<Integer, Double> fractionsAtBase = new HashMap<>(input.getNodeLabelMap().get(this.baseVariable));

        final double size;
        final Map<Integer, Double> fractionsAtTarget = new HashMap<>(fractionsAtBase.size());
        if (input.isInitial()) {
            // We know the exact size
            size = this.dbProps.relationships(-1, this.relationshipTypes, -1, this.direction);
            // If the result is not empty, we know the exact probabilities on base and target variable.
            if (size > 0.0d) {
                for (int l : this.dbProps.labels()) {
                    fractionsAtBase.put(l, this.dbProps.relationships(l, this.relationshipTypes, -1, this.direction) / size);
                }
                for (int l : this.dbProps.labels()) {
                    fractionsAtTarget.put(l, this.dbProps.relationships(-1, this.relationshipTypes, l, this.direction) / size);
                }
            } else {
                // If the initial result size is empty (there are no nodes in the db)
                // we initialize the fractions at the target variable with 0
            	// (logically, any value would be correct, but technically 0 avoids some problems)
                for (int l : this.dbProps.labels()) {
                    fractionsAtTarget.put(l, 0.0d);
                }
            }
        } else {
            final double estimatedTotalDegree = this.estimateDegree(input);

            // if the estimated degree is zero it makes no sense to update the node label maps
            if (estimatedTotalDegree > 0.0d) {
                if (input.anyNodeMatchedBy(this.targetVariable)) {
                    // Compute new label fractions at the existing variable
                    // if no node has label l it makes no sense to try to update its node label fraction
                    this.dbProps.labels().stream().filter(l -> this.dbProps.nodes(l) > 0.0d).forEach(l -> {
                        final Map<Integer, Double> oldFractionsAtTarget = input.getNodeLabelMap().get(this.targetVariable);
                        final double estimatedDegreeToL = this.estimateDegree(input, l) * oldFractionsAtTarget.get(l) / this.dbProps.nodes(l);
                        // Logically it always holds that estimatedDegreeToL <= estimatedTotalDegree, but this may fail due to rounding errors.
                        fractionsAtTarget.put(l, Math.min(1.0d, estimatedDegreeToL / estimatedTotalDegree));
                    });
                } else {
                    // Compute label fractions at the new variable
                    for (int l : this.dbProps.labels()) {
                        // Neo4j estimates R(l1, T, l2) as min{ R(l1, T, *), R(*, T, l2) }
                        // However this is only an upper bound and often too high. It causes too many labels to have
                        // fraction 1 at the target variable in the node label map.
                        fractionsAtTarget.put(l, this.estimateDegree(input, l) / estimatedTotalDegree);
                    }
                }

                // To calculate the new fractions at the base variable we install a node label selection on the input
                // for every label and then reestimate the expansion degree.
                for (int l : this.dbProps.labels()) {
                    final GAResultProperties inputPropsWithSelection =
                            new NodeLabelSelection(this.dbProps, this.baseVariable, l).computeLogicalProperties(Collections.singletonList(input));
                    if (inputPropsWithSelection.getSize() > 0.0d) {
                        // if the selection result is empty, the node label map has no meaning and can therefore not be
                        // used to estimate the degree (would compute wrong values for the new fractions)
                        fractionsAtBase.put(l,
                                Math.min(1.0d, this.estimateDegree(inputPropsWithSelection, -1) * input.getNodeLabelMap().get(this.baseVariable).get(l) / estimatedTotalDegree));
                    } else {
                        fractionsAtBase.put(l, 0.0d);
                    }
                }
            } else {
                // If the estimated degree is 0 (= empty result) we initialize the fractions at the
                // target variable with 0 (logically, any value would be correct, but technically 0 avoids
                // some problems
                for (int l : this.dbProps.labels()) {
                    fractionsAtTarget.put(l, 0.0d);
                }
            }

            size = input.getSize() * estimatedTotalDegree;
        }

        final Map<String, Map<Integer, Double>> nodeLabelMap = new HashMap<>(input.getNodeLabelMap());
        nodeLabelMap.put(this.baseVariable, fractionsAtBase);
        nodeLabelMap.put(this.targetVariable, fractionsAtTarget);

        return new GAResultProperties(nodeLabelMap, relationshipTypeMap, size, false);
    }

    @Override
    public int hash() {
        return Objects.hash(this.baseVariable, this.relationshipVariable, this.targetVariable);
    }

    @Override
    public boolean eq(final GALogicalOperator other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Expand)) {
            return false;
        }
        final Expand otherExpand = (Expand) other;
        return otherExpand.getBaseVariable().equals(this.baseVariable)
                && otherExpand.getRelationshipVariable().equals(this.relationshipVariable)
                && otherExpand.getTargetVariable().equals(this.targetVariable);
    }

    /**
     * Returns the string that matches the nodes to be expanded by this operator.
     *
     * @return which nodes should be expanded from the input
     */
    public String getBaseVariable() {
        return baseVariable;
    }


    /**
     * Returns the string that matches the relationships found by this expansion.
     *
     * @return how the relationships found by this expansion are matched
     */
    public String getRelationshipVariable() {
        return relationshipVariable;
    }

    /**
     * Returns the types matched relationships may have.
     *
     * @return which types are allowed for the relationship variable
     */
    public Set<Integer> getRelationshipTypes() {
        return relationshipTypes;
    }

    /**
     * Returns the string that matches the new nodes found by this expansion.
     *
     * @return how the new nodes found by this expansion are matched
     */
    public String getTargetVariable() {
        return targetVariable;
    }

    /**
     * Returns the relationship direction this expansion uses.
     *
     * @return whether this expansion uses incoming or outgoing edges
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * Returns this operator's database properties.
     *
     * @return database properties
     */
    public GADbProperties getDBProperties() {
        return this.dbProps;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Expand_{");
        sb.append(this.getBaseVariable()).append("}")
            .append(this.direction == Direction.INCOMING ? "<-[" : "-[")
            .append(this.getRelationshipVariable());
        if (!this.getRelationshipTypes().contains(-1)) {
            final Iterator<Integer> relTypeIter = this.getRelationshipTypes().iterator();
            if (relTypeIter.hasNext()) {
                sb.append(":").append(relTypeIter.next());
                while (relTypeIter.hasNext()) {
                    sb.append("|").append(relTypeIter.next());
                }
            }
        }
        return sb.append(this.direction == Direction.OUTGOING ? "]->" : "]-")
            .append("{").append(this.getTargetVariable()).append("}").toString();
    }
}
