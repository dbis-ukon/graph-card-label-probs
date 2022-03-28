package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.estimator.GADbProperties;
import kn.uni.dbis.alhd.estimator.GAResultProperties;

import java.util.*;

/**
 * Implementation of the NodeJoin-operator.
 */
public class NodeJoin implements GALogicalOperator {
    /** Database properties. */
    private final GADbProperties dbProps;

    /**
     * Constructs a new NodeJoin-operator.
     *
     * The NodeJoin-operator joins two sets of subgraphs. For each pair of subgraphs,
     * their common nodes are merged.
     *
     * If the two sets of subgraphs have no common node variables, a cross product
     * will be generated.
     *
     * @param dbProps database properties
     */
    public NodeJoin(final GADbProperties dbProps) {
        this.dbProps = dbProps;
    }

    @Override
    public GAResultProperties computeLogicalProperties(final List<GAResultProperties> inputProperties) {
        final GAResultProperties leftInput = inputProperties.get(0);
        final GAResultProperties rightInput = inputProperties.get(1);
        final Map<String, Map<Integer, Double>> nodeLabelMap = new HashMap<>(leftInput.getNodeLabelMap());
        final Map<String, Set<Integer>> relationshipTypeMap = new HashMap<>(leftInput.getRelationshipTypeMap());

        // Unite the left and right node label maps (unite the label sets in case of colliding variables).
        rightInput.getNodeLabelMap().entrySet().forEach(e1 -> nodeLabelMap.merge(e1.getKey(), e1.getValue(),
            (leftFractions, rightFractions) -> {
                final Map<Integer, Double> fractions = new HashMap<>(this.dbProps.labels().size());
                for (int l : this.dbProps.labels()) {
                    final double leftP = leftFractions.get(l);
                    final double rightP = rightFractions.get(l);
                    fractions.put(l, leftP * rightP + (1.0d - leftP) * rightP + leftP * (1.0d - rightP));
                }
                return fractions;
            })
        );

        // The relationship variables are guaranteed to be disjunct, so simply insert one of the maps into the other.
        relationshipTypeMap.putAll(rightInput.getRelationshipTypeMap());

        // Calculate join cardinality estimate.
        final double totalNodes = this.dbProps.nodes(-1);
        final double leftInputSize = leftInput.getSize();
        final double rightInputSize = rightInput.getSize();
        final Set<String> commonVars = new HashSet<>(leftInput.getNodeVariables());
        commonVars.retainAll(rightInput.getNodeVariables());
        final double matchProbability = commonVars.isEmpty() ? 1D : commonVars.size() / totalNodes;

        return new GAResultProperties(nodeLabelMap, relationshipTypeMap,
                leftInputSize * rightInputSize * matchProbability, false);
    }

    @Override
    public int hash() {
        // this is sufficient, because all node joins do the same thing.
        return 0;
    }

    @Override
    public boolean eq(final GALogicalOperator other) {
        return this == other || other instanceof NodeJoin;
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
        return "NodeJoin";
    }
}
