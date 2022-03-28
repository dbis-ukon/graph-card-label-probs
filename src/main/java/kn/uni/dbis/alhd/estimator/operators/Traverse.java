package kn.uni.dbis.alhd.estimator.operators;

import kn.uni.dbis.alhd.estimator.GADbProperties;
import kn.uni.dbis.alhd.estimator.GAResultProperties;

import java.util.*;
import java.util.Map.Entry;

public class Traverse implements GALogicalOperator {
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

    /**
     * Constructs a new Traverse-operator.
     *
     * @param dbProps database properties
     * @param baseVariable which nodes should be expanded from the input
     * @param relationshipVariable how to match the relationships found by this expansion
     * @param relationshipTypes the types of relationships found by this expansion
     *                          (the empty set represents all types)
     * @param targetVariable how to match the new nodes found by this expansion
     */
    public Traverse(
            final GADbProperties dbProps,
            final String baseVariable,
            final String relationshipVariable,
            final Set<Integer> relationshipTypes,
            final String targetVariable) {
        this.dbProps = dbProps;
        this.baseVariable = baseVariable;
        this.relationshipVariable = relationshipVariable;
        this.relationshipTypes = relationshipTypes;
        this.targetVariable = targetVariable;
    }

	@Override
	public GAResultProperties computeLogicalProperties(final List<GAResultProperties> inputProperties) {
		final GAResultProperties inputProps = inputProperties.get(0);
        final Map<String, Map<Integer, Double>> inputNodeLabelMap = inputProps.getNodeLabelMap();
        final Map<Integer, Double> base = inputNodeLabelMap.get(this.baseVariable);
        final Map<Integer, Double> target = inputNodeLabelMap.get(this.targetVariable);
        final Map<String, Map<Integer, Double>> nodeLabelMap = new HashMap<>(inputNodeLabelMap);
        double size = 0;
        final Set<Integer> relTypes = new HashSet<>();
        for (final Entry<Integer, Double> from : base.entrySet()) {
            final double fromRatio = from.getValue();
            if (fromRatio > 0) {
                final int fromLabel = from.getKey();
            }
        }
        final Map<String, Set<Integer>> relTypeMap = new HashMap<>(inputProps.getRelationshipTypeMap());
        relTypeMap.put(this.relationshipVariable, relTypes);
		return new GAResultProperties(nodeLabelMap, relTypeMap, size, false);
	}

	@Override
	public int hash() {
		return Arrays.hashCode(new Object[] {
		    this.baseVariable,
		    this.relationshipVariable,
		    this.relationshipTypes,
		    this.targetVariable
		});
	}

	@Override
	public boolean eq(final GALogicalOperator other) {
	    if (!(other instanceof Traverse)) {
	        return false;
	    }
	    final Traverse that = (Traverse) other;
		return this.baseVariable.equals(that.baseVariable)
		        && this.relationshipTypes.equals(that.relationshipTypes)
		        && this.relationshipVariable.equals(that.relationshipVariable)
		        && this.targetVariable.equals(that.targetVariable);
	}

}
